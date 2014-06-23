#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
#include <string.h>
#include "HeapDBFile.h"
#include "SortedDBFile.h"
using namespace std;
extern struct AndList *final;
int DBFile::havnop=0;
//Constructor
DBFile::DBFile () {

}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
	if(f_type == heap){
	//Sets the file path
		myInternalVar =	new HeapDBFile();
	}
	else if(f_type == sorted){
		myInternalVar = new SortedDBFile();
	}

	else{
	cout << "\nType mismatch\n";
	return 0;
	}
	myInternalVar->Create(f_path, f_type, startup);
	return 1;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	myInternalVar->Load(f_schema, loadpath);
}

int DBFile::Open (char *f_path) {
	char *mf = new char[strlen(f_path)+4];
	strcpy(mf,f_path);
	strcat(mf,".meta");
	red.open(mf);
	string line;
	if(red.is_open()){	
		getline(red,line);
		if(strcmp(line.c_str(),"Heap")==0){
			cout << "Going to heap";
			myInternalVar = new HeapDBFile();
			myInternalVar->Open(f_path);
			red.close();			
			return 1;
		}
		else if(strcmp(line.c_str(),"Sorted")==0){
			myInternalVar = new SortedDBFile();
			myInternalVar->Open(f_path);
			red.close();			
			return 1;		
		}
		else{
			cout<<"\n Unable to find file";
			red.close();			
			return 0;
		}
	}

}

void DBFile::MoveFirst () {
	myInternalVar->MoveFirst();
}

int DBFile::Close () {
	myInternalVar->Close();
}

void DBFile::Add (Record &rec) {
	myInternalVar->Add(rec);
	havnop++;
}

int DBFile::GetNext (Record &fetchme) {
	myInternalVar->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	myInternalVar->GetNext(fetchme,cnf,literal);
}
