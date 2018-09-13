#!/bin/bash

iquery -aq "remove(de_rle_test_1)" >> /dev/null 2>&1
iquery -aq "remove(de_rle_test_2)" >> /dev/null 2>&1
iquery -aq "remove(de_rle_test_3)" >> /dev/null 2>&1

iquery -aq "remove(de_rle_test_1_d)" >> /dev/null 2>&1
iquery -aq "remove(de_rle_test_2_d)" >> /dev/null 2>&1
iquery -aq "remove(de_rle_test_3_d)" >> /dev/null 2>&1

iquery -aq "create array de_rle_test_1 <val:int64>[x=1:10:0:10]"
iquery -anq "store(build(de_rle_test_1, x), de_rle_test_1)"

iquery -aq "create array de_rle_test_2 <val1:int64, val2:float, val3:uint8>[x=1:10:0:10]"
iquery -anq "store( 
              apply( 
               build( <val1:int64>[x=1:10:0:10], x), 
               val2, float(random()/1234567.89), 
               val3, uint8(iif(x<5, 0, 1))
              ), 
              de_rle_test_2
             )"

iquery -aq "create array de_rle_test_3 <val1:int64, val2:float, val3:uint8>[x=1:1000000:0:100000]"
iquery -anq "store( 
              apply( 
               build( <val1:int64>[x=1:1000000:0:100000], x), 
               val2, float(random()%3), 
               val3, uint8(iif((x/100)%2=0, 0, 1))
              ), 
              de_rle_test_3
             )"

iquery -anq "store( de_rle(de_rle_test_1), de_rle_test_1_d)"
iquery -anq "store( de_rle(de_rle_test_2), de_rle_test_2_d)"
iquery -anq "store( de_rle(de_rle_test_3), de_rle_test_3_d)"

iquery -otsv -aq "op_count(join(de_rle_test_1, de_rle_test_1_d))"
iquery -otsv -aq "op_count(join(de_rle_test_2, de_rle_test_2_d))"
iquery -otsv -aq "op_count(join(de_rle_test_3, de_rle_test_3_d))"

iquery -otsv -aq "op_count(filter(join(de_rle_test_1 as A, de_rle_test_1_d as B), A.val != B.val  ) )"
iquery -otsv -aq "op_count(filter(join(de_rle_test_2 as A, de_rle_test_2_d as B), A.val1!=B.val1 or A.val2!=B.val2 or A.val3!=B.val3))"
iquery -otsv -aq "op_count(filter(join(de_rle_test_3 as A, de_rle_test_3_d as B), A.val1!=B.val1 or A.val2!=B.val2 or A.val3!=B.val3))"

