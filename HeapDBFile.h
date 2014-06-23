#ifndef HEAPDBFILE_H
#define HEAPDBFILE_H

#include <fstream>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"

class HeapDBFile:public GenericDBFile{
	private:
		ofstream myf;
		ifstream red;
		File fileBuffer; 
		Page pageBuffer;
		char *filename;
		//Current record pointer
		Record currentRecord;
		Schema *tempschema;
		off_t pageno;//For current page number confusion
	
	public:
		HeapDBFile();
		int Create (char *fpath, fType file_type, void *startup);	
		int Open (char *fpath);
		int Close ();
		void Load (Schema &myschema, char *loadpath);
		void MoveFirst ();
		void Add (Record &addme);
		int GetNext (Record &fetchme);
		int GetNext (Record &fetchme, CNF &cnf, Record &literal);
		
	};
#endif	
	
