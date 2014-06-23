#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "DBFile.h"

using namespace std;

class BigQ {
	typedef struct {
	Pipe *pipe;
	OrderMaker *order;
	bool print;
	bool write;
	}test;
	DBFile db;
	File fb;
	Page pb;
	test tu;
        char* filename;
	Pipe *input;
	Pipe *output;
	static OrderMaker *sortin;
        static OrderMaker *sortl;
        static OrderMaker *sortr;
	int runl;
        pthread_t worker;

public:
	class Cont{
                public:
		Record r;
                Page p;
		int current;
		int limits;
		int index;
        };
	class CompareForQ{
	OrderMaker *sor; 
	public:
	CompareForQ(OrderMaker *o){
		sor = o;
	}
	bool operator()(Cont *left, Cont *right){        
		ComparisonEngine ceng;
	        int a = ceng.Compare(&left->r,&right->r,sor);
	        if(a<=0)
	       	        a=0;
	       	else
	               	a=1;
		    
		        return a;
		}
	};
        BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen, char* fn);
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
	static int Comp(const void *left,const void *right);
	static int Compl(const void *left,const void *right);
	static int Compr(const void *left,const void *right);
	static void *conshelper(void *arg);
	void *cons(void);
	int merging(char *f,vector<Cont*> v);
        void WaitUntilDone ();
};

#endif
