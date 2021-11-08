cd ../src/shardkv/
cat ee.log | grep 'G100-' > g100.log
cat ee.log | grep ' CLNT ' > client.log
cat g100.log | grep 'G100-S0' > g100-s0.log
cat g100.log | grep 'G100-S1' > g100-s1.log
cat g100.log | grep 'G100-S2' > g100-s2.log
cat ee.log | grep 'G101-' > g101.log
cat g101.log | grep 'G101-S0' > G101-s0.log
cat g101.log | grep 'G101-S1' > G101-s1.log
cat g101.log | grep 'G101-S2' > G101-s2.log
cat ee.log | grep 'G102-' > g102.log
cat g102.log | grep 'G102-S0' > G102-s0.log
cat g102.log | grep 'G102-S1' > G102-s1.log
cat g102.log | grep 'G102-S2' > G102-s2.log
