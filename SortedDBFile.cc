#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "SortedDBFile.h"
#include <iostream>
#include<stdio.h>
#include <fstream>
#include <queue>
#include <string.h>

using namespace std;
extern struct AndList *final;
int SortedDBFile::sonop;
int SortedDBFile::fonop;
int SortedDBFile::havnop;
//Constructor

SortedDBFile::SortedDBFile() {

}

int SortedDBFile::nor;

int SortedDBFile::Create(char *f_path, fType f_type, void *startup) {
    pageBuffer.EmptyItOut();
    filename = f_path;
    sonop = 0;
    curmode = 0;
    fileBuffer.Open(0, filename);
    //BigQ bq (*in,*out,so,rl);
    //cout<<"\n After file";
    //Add blank page as first page in file
    //cout << "File Created";
    SortInfo *s = (SortInfo *) startup;
    char *mf = new char[strlen(filename) + 4];
    strcpy(mf, filename);
    strcat(mf, ".meta");
    nor = 0;
    myf.open(mf);
    myf << "Sorted";
    //cout<<"\n Her you all";
    OrderMaker om = *(OrderMaker *) s->o;
    //s->o->Print();
    myf << "\n" << om;
    myf << "\n" << s->l;
    myf.close();
    return 1;
}
int SortedDBFile::curmode;

void SortedDBFile::Load(Schema &f_schema, char *loadpath) {
    int counter;
    Record temp;
    tempschema = &f_schema;
    //A counter to check that the file does not exceed 10000 records 
    FILE *tableFile = fopen(loadpath, "r");
    while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {
        Add(temp);
    }
}

int SortedDBFile::Open(char *f_path) {
    string mf;
    curmode = 0;
    filename = f_path;
    mf.append(f_path);
    mf.append(".meta");
    red.open(mf.c_str());
    //void *sop;
    bool qoc = 0;
    string line;
    if (red.is_open()) {
        getline(red, line);
        red >> so;
        //sop = &line;
        //so = (OrderMaker *)sop;
        //so.Print();
        //so = (OrderMaker *)line;
        red >> rl;
        //cout <<"cheers "<< rl;
    }
    fileBuffer.Open(fileBuffer.GetLength() - 2, f_path);
    MoveFirst();

    return 1;
}

void SortedDBFile::MoveFirst() {
    if (fileBuffer.GetLength() > 1) {
        fileBuffer.GetPage(&pageBuffer, 0);
        pageno = 0;
        if (curmode == 1)
            MergeRecs();
    }
}

int SortedDBFile::Close() {
    if (curmode == 1)
        MergeRecs();
    return fileBuffer.Close();
}

void SortedDBFile::Add(Record &rec) {
    havnop++;
    //	nor++;
    if (curmode == 0) {
        curmode = 1;
        in = new Pipe(9000);
        out = new Pipe(9000);
        in->Insert(&rec);
        bq = new BigQ(*in, *out, so, rl);
    } else {// if(curmode==1){
        //cout<<"\nPls tell";
        in->Insert(&rec);
    }
    if (nor == 9000) {
        MergeRecs();
        nor = 0;
    }

}

int SortedDBFile::GetNext(Record &fetchme) {
    if (curmode == 1)
        MergeRecs();
    if (fileBuffer.GetLength() == 0) {
        cout << "\n No files present";
        return 0;
    }
    if (pageBuffer.GetFirst(&fetchme) == 1) {
        //cout<<"\nLanes";
        return 1;
    } else {
        //cout<<"\nManesa";		
        if (pageno < fileBuffer.GetLength() - 2) {
            //cout<<"\nas";
            fileBuffer.GetPage(&pageBuffer, ++pageno);
            pageBuffer.GetFirst(&fetchme);
            return 1;
        } else {
            return 0;
        }
    }

}

void SortedDBFile::formQuery(OrderMaker &so, CNF &query, OrderMaker &queryo, OrderMaker &cnfo, bool &flagq) {
    //	cout<<"\n AM in"<<so.numAtts;
    queryo.numAtts = 0;
    cnfo.numAtts = 0;
    for (int i = 0; i < (so.numAtts); i++) {
        typeof (so.whichTypes[i]) & att = so.whichTypes[i];
        typeof (so.whichTypes[i]) & type = so.whichTypes[i];
        int j = findAttrIn(att, query);
        if (j >= 0) { // found this attribute in query CNF
            queryo.whichAtts[queryo.numAtts] = att;
            queryo.whichTypes[queryo.numAtts] = type;
            cnfo.whichAtts[cnfo.numAtts] = i;
            cnfo.whichTypes[cnfo.numAtts] = type;
            ++queryo.numAtts;
            ++cnfo.numAtts;
            //			cout<<"\nmamsi";
            queryo.Print();
            cnfo.Print();
        } else {
            //			cout<<"\nAapu";	
            return;
        } // don't search further
        if(so.numAtts == queryo.numAtts) {
            flagq = true;
            for(int i = 0; i < so.numAtts ; i++) {
                if(so.whichAtts[i]!=queryo.whichAtts[i]) {
                    flagq =false;
                    break;
                }
            }
        }
        else {
            flagq = false;
        }
    }
}

int SortedDBFile::findAttrIn(int att, CNF &query) {
    //	cout<<"\nlane"<<query.numAnds;
    for (size_t i = 0; i < (query.numAnds); ++i) {
        typeof (query.orList[i]) & clause = query.orList[i];
        typeof (query.orLens[i]) & orLen = query.orLens[i];
        if (orLen == 1) {
            Comparison &cmp = clause[0];
            if (cmp.op == Equals && (((cmp.whichAtt1 == att) &&(cmp.operand2 == Literal)) || (cmp.whichAtt2 == att) &&(cmp.operand1 == Literal)))
                return i;

            else if (cmp.op == GreaterThan && (((cmp.whichAtt1 == att) &&(cmp.operand2 == Literal)) || (cmp.whichAtt2 == att) &&(cmp.operand1 == Literal)))
                return i;

            else if (cmp.op == LessThan && (((cmp.whichAtt1 == att) &&(cmp.operand2 == Literal)) || (cmp.whichAtt2 == att) &&(cmp.operand1 == Literal)))
                return i;
        }
    }
    return -1;
}

int SortedDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    //cout<<"\n INga " << so.numAtts;
    if (curmode == 1)
        MergeRecs();
    ComparisonEngine comp;
    //cout<<"\nennamo";	
    if (fileBuffer.GetLength() == 0) {
        cout << "\n The file is not created";
        return 0;
    }
    //cout<<"\njik"<<mid<<" "<<high;
    //cnfo.Print();
    //queryo.Print();
    if (qoc == 0) {
        bool flagq = false;
        int mid;
        int high;
        high = fileBuffer.GetLength() - 2;
        mid = high / 2;
        formQuery(so, cnf, queryo, cnfo, flagq);
        if(flagq == true){
                BinarySearch(&queryo, literal, &cnfo, 0, mid, high);
        }
        else {
//            fileBuffer.GetPage(&pageBuffer, 0);
//            pageno =1;
        }
        qoc = 1;
    }
    while (GetNext(fetchme)) {
        if (comp.Compare(&fetchme, &literal, &cnf)) {
            return 1;
        }
    }
    return 0;
    //BinarySearch(
}

void SortedDBFile::BinarySearch(OrderMaker *queryo, Record &literal, OrderMaker *cnfo, int low, int mid, int high) {

    //cout<<"\nlion"<<low<<mid<<high;	
    Record chuma;
    fileBuffer.GetPage(&pageBuffer, mid);
    pageBuffer.GetFirst(&chuma);
    int newmid;
    ComparisonEngine ceng;
    int diff = high - low;
    if (diff <= 2) {

        //cout<<"\nliger";	
        pageno = low;
        fileBuffer.GetPage(&pageBuffer, pageno);
        qoc = 1;
        return;
    }
    int check = ceng.Compare(&literal, cnfo, &chuma, queryo);
    if (check == 0) {
        //cout<<"\nlhat";	
        pageno = mid;
        fileBuffer.GetPage(&pageBuffer, pageno);
        return;
    } else if (check == -1) {
        //cout<<"\nhas";
        newmid = (low + mid) / 2;
        BinarySearch(queryo, literal, cnfo, low, newmid, mid);
        return;
    } else if (check == 1) {
        //cout<<"\nmads"<<low<<mid<<high;
        newmid = ((high + mid) / 2) + 1;
        //cout<<"Apadina ithu "<<newmid;		
        BinarySearch(cnfo, literal, queryo, mid, newmid, high);
        return;
    }
}

void SortedDBFile::MergeRecs() {
    fileBuffer.Close();
    //cout<<"\nAhem";	
    if (!bq)
        return;
    in->ShutDown();
    ComparisonEngine ceng;
    Record tr;
    //cout <<"\n Warewa  "<<filename;
    priority_queue <Conta *, vector<Conta *>, CompareForQ> pq(&so);
    Conta *tempo = new Conta;
    Conta *con = new Conta[2];
    fonop = 1;
    //sprintf (outfile, "%s.bigq", filename);
    //if(sof.GetLength()==0){
    //	sof.Open(0,filename);
    //}
    //else{	
    //cout << "Sonosp" << sonop;

    //        sof.Open(sonop,filename);
    //	//}
    //	//cout<<"\nOut file " <<" " << sof.GetLength()<<"     ";
    //	int sono=0;
    //	//cout<<"\nTHe magical numbers are " << sono << "  "<< sof.GetLength()-1;
    //	
    //	while(sono < sof.GetLength()-1){
    //		sop.EmptyItOut();		
    //		sof.GetPage(&sop, sono);
    //		sof.Close();
    //		fileBuffer.Open(fonop++,"tempsort.bin");
    //		fileBuffer.AddPage(&sop,sono);
    //		fileBuffer.Close();
    //		sono++;
    //		sof.Open(sonop,filename);
    //	}
    //	sop.EmptyItOut();
    //	sof.Close();
    char *f = "tempsort.bin";
    remove(f);
    rename(filename,f);
    int sono = 0;
    int err = 0;
    int i = 0;
    int pageno = 0;
    con[0].index = 0;
    con[1].index = 1;
    int checki = 0;
    if (out->Remove(&con[0].r)) {
        pq.push(&con[0]);
    }
    int saye = 0;
    fileBuffer.Open(fonop, "tempsort.bin");
    cout << "\n" << pageno << "        " << fonop << "    " << fileBuffer.GetLength();
    if (pageno < fileBuffer.GetLength() - 1) {
        fileBuffer.GetPage(&pageBuffer, pageno);
        pageBuffer.GetFirst(&con[1].r);
        pq.push(&con[1]);
    }
    fileBuffer.Close();
    sonop = 0;
    int check = 0;
    int counting = 0;
    //cout<<"\nellai"<<fonop;
    fileBuffer.Open(fonop, "tempsort.bin");
    while (!pq.empty()) {
        tempo = pq.top();
        int tempind = tempo->index;
        //cout <<"teminp" << tempind;
        pq.pop();

        if (sop.Append(&tempo->r) == 0) {
            sof.Open(sonop++, filename);
            sof.AddPage(&sop, sono);
            sof.Close();
            sono++;
            sop.EmptyItOut();
            sop.Append(&tempo->r);
        }

        if (tempind == 0) {
            //cout<<"\n AM in remove";
            if (out->Remove(&con[0].r)) {
                check++;
                pq.push(&con[0]);
            }
            //cout<<"\npq size is "<<pq.size();
        } else {
            
            if (pageBuffer.GetFirst(&con[1].r) == 0) {

                if (pageno < fileBuffer.GetLength() - 2) {
                    cout << endl << counting++ << " : ";
                    fileBuffer.GetPage(&pageBuffer, ++pageno);
                    pageBuffer.GetFirst(&con[1].r);
                    pq.push(&con[1]);
                    cout << counting;
                }
            } else {
                pq.push(&con[1]);
            }

        }
    }
    fileBuffer.Close();
    sof.Open(sonop++, filename);
    sof.AddPage(&sop, sono);
    sop.EmptyItOut();
    //out->ShutDown();
    delete in;
    delete out;
    delete bq;
    curmode = 0;
    sof.Close();
    fileBuffer.Open(1, filename);
    //	fileBuffer.Close();*/
}
