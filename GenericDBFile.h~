#ifndef GENERICDBFILE_H
#define GENERICDBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
typedef enum {heap, sorted, tree} fType;
typedef fType myType;

class GenericDBFile{

	public:
		GenericDBFile(){};
		//Destructor alone has to be defined for pure virtual functions which cost you 4 hours do not forget 
		virtual ~GenericDBFile(){}		
		virtual int Create (char *fpath, fType file_type, void *startup)=0;
		virtual int Open (char *fpath)=0;
		virtual int Close ()=0;
		virtual void Load (Schema &myschema, char *loadpath){}
		virtual void MoveFirst ()=0;
		virtual void Add (Record &addme)=0;
		virtual int GetNext (Record &fetchme)=0;
		virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal)=0;
};

#endif
