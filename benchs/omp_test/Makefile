module: 
	g++ -O3 -g -std=c++20 -I../../arch/x64 -I../.. -I ../../include -static -lgomp -o omp_test omp_test.cc -fopenmp
	#g++ -O3 -g -std=c++20 -I../../arch/x64 -I../.. -I ../../include -fPIC -shared -rdynamic -lgomp -o omp_test omp_test.cc -fopenmp

linux:
	g++ -O3 -g -std=c++20 -lgomp -o omp_test omp_test.cc -fopenmp

all: module

clean:
	        rm omp_test
