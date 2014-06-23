#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "HeapDBFile.h"
#include <iostream>
#include <fstream>
#include <string.h>

using namespace std;
extern struct AndList *final;

//Constructor
HeapDBFile::HeapDBFile () {

}

int HeapDBFile::Create (char *f_path, fType f_type, void *startup) {
	pageBuffer.EmptyItOut();
	filename = f_path; 
	fileBuffer.Open(0, filename);
	cout<<"\n After file";
        //Add blank page as first page in file
	cout << "File Created";
	pageno = 0;
	char *mf= new char[strlen(filename)+4];
	strcpy(mf,filename);
//	cout << mf;
	strcat(mf,".meta");
	myf.open(mf);
	myf<<"Heap";
	myf.close();		
	return 1;		
}

void HeapDBFile::Load (Schema &f_schema, char *loadpath) {
	int counter;
	Record temp;
	tempschema = &f_schema;
	//A counter to check that the file does not exceed 10000 records 
	FILE *tableFile = fopen (loadpath, "r");
	while (temp.SuckNextRecord (&f_schema, tableFile) == 1) {
		//Whenever the page is full the pagebuffer stores that page in the filebuffer
		if(pageBuffer.Append(&temp) == 0)			
		{
			if(fileBuffer.GetLength()==0){
			 	fileBuffer.AddPage(&pageBuffer, 0);
				pageno=1;
			}
			else{	
			fileBuffer.AddPage(&pageBuffer, pageno);
			pageno++;
			}
			pageBuffer.EmptyItOut();
			pageBuffer.Append(&temp);
		}
			
	}
	//Store lastpage in file
	fileBuffer.AddPage(&pageBuffer, pageno);
	cout << "\n Page Number is "<< pageno << fileBuffer.GetLength();
	pageBuffer.EmptyItOut();
}

int HeapDBFile::Open (char *f_path) {	
		
	fileBuffer.Open(fileBuffer.GetLength()-2, f_path);
        MoveFirst();
	cout <<"Loading in heap";
	return 1;
}

void HeapDBFile::MoveFirst () {
    if(fileBuffer.GetLength() > 1) {
	fileBuffer.GetPage(&pageBuffer,0);
	pageno = 0;
    }
}

int HeapDBFile::Close () {
	return fileBuffer.Close();
}

void HeapDBFile::Add (Record &rec) {
	cout << "\n IN add next";
	if(fileBuffer.GetLength()==0){
		pageBuffer.Append(&rec);
		fileBuffer.AddPage(&pageBuffer, 0);
		return;
	}
	fileBuffer.GetPage(&pageBuffer,fileBuffer.GetLength()-2);
	if(pageBuffer.Append(&rec))			
		{
			cout <<"\n In if";
			fileBuffer.AddPage(&pageBuffer, fileBuffer.GetLength()-2);
			cout << "\n bREAKING here";
			pageBuffer.EmptyItOut();
			cout <<"\n Or herer";
		}	
	//if page is full write to new page
	else
	{	
		cout<< "\n IN else                                    lkklk                            lll llll";
		fileBuffer.AddPage(&pageBuffer, fileBuffer.GetLength()-2);
		pageBuffer.EmptyItOut();
		pageBuffer.Append(&rec);
		fileBuffer.AddPage(&pageBuffer, fileBuffer.GetLength()-1);
		pageBuffer.EmptyItOut();	
	}
}

int HeapDBFile::GetNext (Record &fetchme) {
	if(fileBuffer.GetLength() == 0){
		cout << "\n No files present";
		return 0;	
	}
	if(pageBuffer.GetFirst(&fetchme) == 1)
	{	
		return 1;
	}
	else
	{	
		cout << "\nSee here my dear\n";
		if(pageno < fileBuffer.GetLength()-2)
		{		
			fileBuffer.GetPage(&pageBuffer,++pageno);
			pageBuffer.GetFirst(&fetchme);
			return 1;
		}
		else
		{
		return 0;				
		}
	}
	
}

int HeapDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comp;
	while(GetNext(fetchme))
	{
	if (comp.Compare (&fetchme, &literal, &cnf))
        {	
		return 1;
	}
	}
	return 0;	
}
