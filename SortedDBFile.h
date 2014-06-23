#ifndef SORTEDDBFILE_H
#define SORTEDDBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"
#include "BigQ.h"

class SortedDBFile:public GenericDBFile{
	private:
		ofstream myf;
		ifstream red;
		File fileBuffer; 
		Page pageBuffer;
		File sof;
		Page sop;
		char *filename;
		//Current record pointer
		Record currentRecord;
		Schema *tempschema;
		struct SortInfo{OrderMaker *o; int l;};
		off_t pageno;//For current page number confusion
		static int curmode;//To check whether it is in the read or write mode
		static int nor;	
		static int sonop,fonop,havnop;	
		Pipe *in;
		Pipe *out;
		BigQ *bq;
		int rl;
		OrderMaker so;
		OrderMaker queryo;
		OrderMaker cnfo;
		bool qoc;
	
	public:
		class Conta{
                public:
		Record r;
                int index;
		};
		class CompareForQ{
		OrderMaker *sor; 
		public:
		CompareForQ(OrderMaker *o){
			sor = o;
		}
		bool operator()(Conta *left, Conta *right){        
		ComparisonEngine ceng;
	        int a = ceng.Compare(&left->r,&right->r,sor);
	        if(a<=0)
	       	        a=0;
	       	else
	               	a=1;
		    
		        return a;
		}
		};
		SortedDBFile();
		void MergeRecs();
		int Create (char *fpath, fType file_type, void *startup);	
		int Open (char *fpath);
		int Close ();
		void Load (Schema &myschema, char *loadpath);
		void MoveFirst ();
		void Add (Record &addme);
		int GetNext (Record &fetchme);
		int GetNext (Record &fetchme, CNF &cnf, Record &literal);
		void formQuery(OrderMaker &so,CNF &query, OrderMaker &queryo, OrderMaker &cnfo, bool &flagq);
		int findAttrIn(int att, CNF &query);
		void BinarySearch(OrderMaker *queryo, Record &literal, OrderMaker *cnfo,int low, int mid, int high);
};
#endif	
	
