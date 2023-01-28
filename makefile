build: 
	cc 30_ptop.c -o 30_test.o -l pthread -O3
	cc single_threaded_read.c -o sr.o -l pthread 
	cc multi_threaded_read.c -o mr.o -l pthread
	cc single_threaded_read_and_parse.c -o srp.o -l pthread
	cc multi_threaded_read_and_parse.c -o mrp.o -l pthread
test:
	echo "here are the reference output"
	
	./30_test.o case1/ 1645491600 5000
	./30_test.o case2/ 1645491600 5000
	./30_test.o case3/ 1645491600 5000
	./30_test.o case4/ 1645491600 5000
	./30_test.o case5/ 1645491600 5000

	echo "here are the reference output"

	./test.example case1/ 1645491600 10
	./test.example case2/ 1645491600 10
	./test.example case3/ 1645491600 10
	./test.example case4/ 1645491600 10
	./test.example case5/ 1645491600 10

clean :
	-rm -f *.out *.o
