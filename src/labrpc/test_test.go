package labrpc

import (
	"6.824/mr"
	"hash/fnv"
	"log"
	"os"
	"testing"
)
import "strconv"
import "sync"
import "runtime"
import "time"
import "fmt"

type JunkArgs struct {
	X int
}
type JunkReply struct {
	X string
}

type JunkServer struct {
	mu   sync.Mutex
	log1 []string
	log2 []int
}

func (js *JunkServer) Handler1(args string, reply *int) {
	js.mu.Lock()
	defer js.mu.Unlock()
	js.log1 = append(js.log1, args)
	*reply, _ = strconv.Atoi(args)
}

func (js *JunkServer) Handler2(args int, reply *string) {
	js.mu.Lock()
	defer js.mu.Unlock()
	js.log2 = append(js.log2, args)
	*reply = "handler2-" + strconv.Itoa(args)
}

func (js *JunkServer) Handler3(args int, reply *int) {
	js.mu.Lock()
	defer js.mu.Unlock()
	time.Sleep(20 * time.Second)
	*reply = -args
}

// args is a pointer
func (js *JunkServer) Handler4(args *JunkArgs, reply *JunkReply) {
	reply.X = "pointer"
}

// args is a not pointer
func (js *JunkServer) Handler5(args JunkArgs, reply *JunkReply) {
	reply.X = "no pointer"
}

func (js *JunkServer) Handler6(args string, reply *int) {
	js.mu.Lock()
	defer js.mu.Unlock()
	*reply = len(args)
}

func (js *JunkServer) Handler7(args int, reply *string) {
	js.mu.Lock()
	defer js.mu.Unlock()
	*reply = ""
	for i := 0; i < args; i++ {
		*reply = *reply + "y"
	}
}

type MyServer struct {
	id int
}

func (s *MyServer) Fuck(args string, reply *string) {
	*reply = "fuck! " + args
}

// 坑，struct和方法必须是大写，这样才能调用。。
//返回参数都是指针
func TestMyRPC(t *testing.T) {
	runtime.GOMAXPROCS(4) //GO最大CPU数目

	rn := MakeNetwork()
	defer rn.Cleanup()

	e := rn.MakeEnd("testEnd")

	js := &MyServer{} //RPC方法具体修改这个结构体
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server!", rs)

	rn.Connect("testEnd", "server!")
	rn.Enable("testEnd", true)

	rt := ""
	e.Call("MyServer.Fuck", "abc", &rt)
	fmt.Printf("%s\n", rt)

}

type Task struct {
	Id            int
	Type          uint8
	InputLocation []string
	NReduce       int
}

type Output struct {
	Task           *Task
	Status         uint8
	OutputLocation []string
	P              []int
}

func (s *MyServer) FuckArray(args *Task, output *Output) {
	fmt.Printf("%#v\n", args)
	*output = Output{nil, 0, []string{"abc", "def"}, []int{1, 2, 3}}
	fmt.Printf("%#v\n", output)
}

func (s *MyServer) FuckArray2(args *Task, string2 *[]string) {
	*string2 = []string{"1", "333"}
}

func (s *MyServer) FuckAskForWork(worker string, task *Task) {
	p := s.id
	s.id++
	fmt.Println(p)
	t := Task{p, 0, []string{"abc"}, 1}
	fmt.Printf("%s rt %#v\t*******", worker, t)
	*task = t
}

func TestStrange(t *testing.T) {
	runtime.GOMAXPROCS(4) //GO最大CPU数目

	rn := MakeNetwork()
	defer rn.Cleanup()

	e := rn.MakeEnd("testEnd")

	js := &MyServer{} //RPC方法具体修改这个结构体
	js.id = 1000
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server!", rs)

	rn.Connect("testEnd", "server!")
	rn.Enable("testEnd", true)

	worker := "w-0"
	rt := &Task{}
	for i := 0; i < 10; i++ {
		e.Call("MyServer.FuckAskForWork", worker, rt)
		fmt.Printf("%#v\n", rt)
	}
	fmt.Printf("%#v\n", rt)
}

func TestArray(t *testing.T) {
	runtime.GOMAXPROCS(4) //GO最大CPU数目

	rn := MakeNetwork()
	defer rn.Cleanup()

	e := rn.MakeEnd("testEnd")

	js := &MyServer{} //RPC方法具体修改这个结构体
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server!", rs)

	rn.Connect("testEnd", "server!")
	rn.Enable("testEnd", true)

	rt := &Output{}
	e.Call("MyServer.FuckArray", &Task{}, &rt)
	fmt.Printf("%#v\n", rt)

	rt2 := []string{"abc", "def"}
	e.Call("MyServer.FuckArray2", &Task{}, &rt2)
	fmt.Printf("%s\n", rt2)
}
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
func TestHash(t *testing.T) {
	fmt.Println(ihash("A"))
	fmt.Println(ihash("About"))
	fmt.Println(ihash("ABOUT"))
}

func TestTimer(t *testing.T) {
	fmt.Println((time.Duration(10) * time.Millisecond).Microseconds())
	fmt.Println(time.Now().UnixNano() / 1e6)
	var last = time.Now().UnixNano() / 1e6
	time.Sleep(time.Duration(10) * time.Millisecond)
	fmt.Println(time.Now().UnixNano()/1e6 - last)
}

func TestRename(t *testing.T) {
	f1, _ := os.Create("b.txt")
	f1.Close()
	f2, _ := os.Create("a.txt")
	f2.Close()
	time.Sleep(time.Duration(2) * time.Second)
	err := os.Rename("a.txt", "b.txt")
	if err != nil {
		fmt.Println("dsadsa")
		fmt.Println(err)
	}
}

func TestLogfile(t *testing.T) {
	fmt.Println("初始化log")
	file := "master.log"
	index := 1
	if b, _ := mr.PathExists(file); b {
		file = "worker-0.log"
		fmt.Println("master.log不行 ,现在改为", file)
	}
	//注意，for循环的第一个是初始值
	for b, _ := mr.PathExists(file); b; b, _ = mr.PathExists(file) {
		fmt.Printf("%s 不行\n", file)
		file = fmt.Sprintf("worker-%d.log", index)
		index++
	}
	fmt.Printf("日志文件: %s\n", file)
	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile) // 将文件设置为log输出的文件
	//log.SetPrefix("[qSkipTool]")
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server99", rs)

	rn.Connect("end1-99", "server99")
	rn.Enable("end1-99", true)

	{
		reply := ""
		e.Call("JunkServer.Handler2", 111, &reply)
		if reply != "handler2-111" {
			t.Fatalf("wrong reply from Handler2")
		}
	}

	{
		reply := 0
		e.Call("JunkServer.Handler1", "9099", &reply)
		if reply != 9099 {
			t.Fatalf("wrong reply from Handler1")
		}
	}
}

func TestTypes(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server99", rs)

	rn.Connect("end1-99", "server99")
	rn.Enable("end1-99", true)

	{
		var args JunkArgs
		var reply JunkReply
		// args must match type (pointer or not) of handler.
		e.Call("JunkServer.Handler4", &args, &reply)
		if reply.X != "pointer" {
			t.Fatalf("wrong reply from Handler4")
		}
	}

	{
		var args JunkArgs
		var reply JunkReply
		// args must match type (pointer or not) of handler.
		e.Call("JunkServer.Handler5", args, &reply)
		if reply.X != "no pointer" {
			t.Fatalf("wrong reply from Handler5")
		}
	}
}

//
// does net.Enable(endname, false) really disconnect a client?
//
func TestDisconnect(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server99", rs)

	rn.Connect("end1-99", "server99")

	{
		reply := ""
		e.Call("JunkServer.Handler2", 111, &reply)
		if reply != "" {
			t.Fatalf("unexpected reply from Handler2")
		}
	}

	rn.Enable("end1-99", true)

	{
		reply := 0
		e.Call("JunkServer.Handler1", "9099", &reply)
		if reply != 9099 {
			t.Fatalf("wrong reply from Handler1")
		}
	}
}

//
// test net.GetCount()
//
func TestCounts(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer(99, rs)

	rn.Connect("end1-99", 99)
	rn.Enable("end1-99", true)

	for i := 0; i < 17; i++ {
		reply := ""
		e.Call("JunkServer.Handler2", i, &reply)
		wanted := "handler2-" + strconv.Itoa(i)
		if reply != wanted {
			t.Fatalf("wrong reply %v from Handler1, expecting %v", reply, wanted)
		}
	}

	n := rn.GetCount(99)
	if n != 17 {
		t.Fatalf("wrong GetCount() %v, expected 17\n", n)
	}
}

//
// test net.GetTotalBytes()
//
func TestBytes(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer(99, rs)

	rn.Connect("end1-99", 99)
	rn.Enable("end1-99", true)

	for i := 0; i < 17; i++ {
		args := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
		args = args + args
		args = args + args
		reply := 0
		e.Call("JunkServer.Handler6", args, &reply)
		wanted := len(args)
		if reply != wanted {
			t.Fatalf("wrong reply %v from Handler6, expecting %v", reply, wanted)
		}
	}

	n := rn.GetTotalBytes()
	if n < 4828 || n > 6000 {
		t.Fatalf("wrong GetTotalBytes() %v, expected about 5000\n", n)
	}

	for i := 0; i < 17; i++ {
		args := 107
		reply := ""
		e.Call("JunkServer.Handler7", args, &reply)
		wanted := args
		if len(reply) != wanted {
			t.Fatalf("wrong reply len=%v from Handler6, expecting %v", len(reply), wanted)
		}
	}

	nn := rn.GetTotalBytes() - n
	if nn < 1800 || nn > 2500 {
		t.Fatalf("wrong GetTotalBytes() %v, expected about 2000\n", nn)
	}
}

//
// test RPCs from concurrent ClientEnds
//
func TestConcurrentMany(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer(1000, rs)

	ch := make(chan int)

	nclients := 20
	nrpcs := 10
	for ii := 0; ii < nclients; ii++ {
		go func(i int) {
			n := 0
			defer func() { ch <- n }()

			e := rn.MakeEnd(i)
			rn.Connect(i, 1000)
			rn.Enable(i, true)

			for j := 0; j < nrpcs; j++ {
				arg := i*100 + j
				reply := ""
				e.Call("JunkServer.Handler2", arg, &reply)
				wanted := "handler2-" + strconv.Itoa(arg)
				if reply != wanted {
					t.Fatalf("wrong reply %v from Handler1, expecting %v", reply, wanted)
				}
				n += 1
			}
		}(ii)
	}

	total := 0
	for ii := 0; ii < nclients; ii++ {
		x := <-ch
		total += x
	}

	if total != nclients*nrpcs {
		t.Fatalf("wrong number of RPCs completed, got %v, expected %v", total, nclients*nrpcs)
	}

	n := rn.GetCount(1000)
	if n != total {
		t.Fatalf("wrong GetCount() %v, expected %v\n", n, total)
	}
}

//
// test unreliable
//
func TestUnreliable(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()
	rn.Reliable(false)

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer(1000, rs)

	ch := make(chan int)

	nclients := 300
	for ii := 0; ii < nclients; ii++ {
		go func(i int) {
			n := 0
			defer func() { ch <- n }()

			e := rn.MakeEnd(i)
			rn.Connect(i, 1000)
			rn.Enable(i, true)

			arg := i * 100
			reply := ""
			ok := e.Call("JunkServer.Handler2", arg, &reply)
			if ok {
				wanted := "handler2-" + strconv.Itoa(arg)
				if reply != wanted {
					t.Fatalf("wrong reply %v from Handler1, expecting %v", reply, wanted)
				}
				n += 1
			}
		}(ii)
	}

	total := 0
	for ii := 0; ii < nclients; ii++ {
		x := <-ch
		total += x
	}

	if total == nclients || total == 0 {
		t.Fatalf("all RPCs succeeded despite unreliable")
	}
}

//
// test concurrent RPCs from a single ClientEnd
//
func TestConcurrentOne(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer(1000, rs)

	e := rn.MakeEnd("c")
	rn.Connect("c", 1000)
	rn.Enable("c", true)

	ch := make(chan int)

	nrpcs := 20
	for ii := 0; ii < nrpcs; ii++ {
		go func(i int) {
			n := 0
			defer func() { ch <- n }()

			arg := 100 + i
			reply := ""
			e.Call("JunkServer.Handler2", arg, &reply)
			wanted := "handler2-" + strconv.Itoa(arg)
			if reply != wanted {
				t.Fatalf("wrong reply %v from Handler2, expecting %v", reply, wanted)
			}
			n += 1
		}(ii)
	}

	total := 0
	for ii := 0; ii < nrpcs; ii++ {
		x := <-ch
		total += x
	}

	if total != nrpcs {
		t.Fatalf("wrong number of RPCs completed, got %v, expected %v", total, nrpcs)
	}

	js.mu.Lock()
	defer js.mu.Unlock()
	if len(js.log2) != nrpcs {
		t.Fatalf("wrong number of RPCs delivered")
	}

	n := rn.GetCount(1000)
	if n != total {
		t.Fatalf("wrong GetCount() %v, expected %v\n", n, total)
	}
}

//
// regression: an RPC that's delayed during Enabled=false
// should not delay subsequent RPCs (e.g. after Enabled=true).
//
func TestRegression1(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer(1000, rs)

	e := rn.MakeEnd("c")
	rn.Connect("c", 1000)

	// start some RPCs while the ClientEnd is disabled.
	// they'll be delayed.
	rn.Enable("c", false)
	ch := make(chan bool)
	nrpcs := 20
	for ii := 0; ii < nrpcs; ii++ {
		go func(i int) {
			ok := false
			defer func() { ch <- ok }()

			arg := 100 + i
			reply := ""
			// this call ought to return false.
			e.Call("JunkServer.Handler2", arg, &reply)
			ok = true
		}(ii)
	}

	time.Sleep(100 * time.Millisecond)

	// now enable the ClientEnd and check that an RPC completes quickly.
	t0 := time.Now()
	rn.Enable("c", true)
	{
		arg := 99
		reply := ""
		e.Call("JunkServer.Handler2", arg, &reply)
		wanted := "handler2-" + strconv.Itoa(arg)
		if reply != wanted {
			t.Fatalf("wrong reply %v from Handler2, expecting %v", reply, wanted)
		}
	}
	dur := time.Since(t0).Seconds()

	if dur > 0.03 {
		t.Fatalf("RPC took too long (%v) after Enable", dur)
	}

	for ii := 0; ii < nrpcs; ii++ {
		<-ch
	}

	js.mu.Lock()
	defer js.mu.Unlock()
	if len(js.log2) != 1 {
		t.Fatalf("wrong number (%v) of RPCs delivered, expected 1", len(js.log2))
	}

	n := rn.GetCount(1000)
	if n != 1 {
		t.Fatalf("wrong GetCount() %v, expected %v\n", n, 1)
	}
}

//
// if an RPC is stuck in a server, and the server
// is killed with DeleteServer(), does the RPC
// get un-stuck?
//
func TestKilled(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server99", rs)

	rn.Connect("end1-99", "server99")
	rn.Enable("end1-99", true)

	doneCh := make(chan bool)
	go func() {
		reply := 0
		ok := e.Call("JunkServer.Handler3", 99, &reply)
		doneCh <- ok
	}()

	time.Sleep(1000 * time.Millisecond)

	select {
	case <-doneCh:
		t.Fatalf("Handler3 should not have returned yet")
	case <-time.After(100 * time.Millisecond):
	}

	rn.DeleteServer("server99")

	select {
	case x := <-doneCh:
		if x != false {
			t.Fatalf("Handler3 returned successfully despite DeleteServer()")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Handler3 should return after DeleteServer()")
	}
}

func TestBenchmark(t *testing.T) {
	runtime.GOMAXPROCS(4)

	rn := MakeNetwork()
	defer rn.Cleanup()

	e := rn.MakeEnd("end1-99")

	js := &JunkServer{}
	svc := MakeService(js)

	rs := MakeServer()
	rs.AddService(svc)
	rn.AddServer("server99", rs)

	rn.Connect("end1-99", "server99")
	rn.Enable("end1-99", true)

	t0 := time.Now()
	n := 100000
	for iters := 0; iters < n; iters++ {
		reply := ""
		e.Call("JunkServer.Handler2", 111, &reply)
		if reply != "handler2-111" {
			t.Fatalf("wrong reply from Handler2")
		}
	}
	fmt.Printf("%v for %v\n", time.Since(t0), n)
	// march 2016, rtm laptop, 22 microseconds per RPC
}
