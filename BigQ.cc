#include "BigQ.h"
#include <vector>
#include <pthread.h>
#include <stdlib.h>
#include <iostream>
#include <algorithm>
#include <queue>
#include <string.h>

using namespace std;
OrderMaker * BigQ::sortin;
OrderMaker * BigQ::sortl;
OrderMaker * BigQ::sortr;
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	// read data from in pipe sort them into runlen pages
	
	//char name[] = "temp";
	input = &in;
	output = &out;
	sortin = &sortorder;
	runl = runlen;
	tu={&in, &sortorder, false, false};
        filename = "temp.bin";
	//Record &temp;
	pthread_create(&worker, NULL, BigQ::conshelper, this);
//	pthread_join(worker, NULL);
	//DBFile db;
    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe
		// finally shut down the out pipe
	//out.ShutDown ();
//	pthread_exit(NULL);
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen, char* fn) {
	// read data from in pipe sort them into runlen pages
	//char name[] = "temp";
	input = &in;
	output = &out;
	
	runl = runlen;
	tu={&in, &sortorder, false, false};
        filename = fn;
        if(strcmp(filename,"leftfile.bin") == 0) {
            sortl = &sortorder;
        }
        else {
            sortr = &sortorder;
        }
	//Record &temp;
	pthread_create(&worker, NULL, BigQ::conshelper, this);
	//pthread_join(worker, NULL);
	//DBFile db;
    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe
		// finally shut down the out pipe
	//out.ShutDown ();
//	pthread_exit(NULL);
}
	
void * BigQ :: conshelper(void *obj) {
	//DBFile is not declared in the header since it is a static function and variable access;
//	cout <<"\nsadad";
	return ((BigQ*)obj)->cons();
}

void * BigQ :: cons (void) {
    
     Attribute IA = {"int", Int};
        Attribute SA = {"string", String};
        Attribute DA = {"double", Double};

        Attribute l_att[] = {IA,SA,SA,IA,SA,DA,SA};

        Schema l_sch ("l_sch", 7, l_att);
	//cout << "\n Heyasdad";	
	//db.Create("temp.bin", heap, NULL);
	pb.EmptyItOut();
	fb.Open(0, filename);
        //Add blank page as first page in file
	/*ComparisonEngine ceng;
	char outfile[100];
	if (t->write) {
		sprintf (outfile, "%s.bigq", rel->path ());
		dbfile.Create (outfile, heap, NULL);
	}
	int err = 0;
	int i = 0;
	Record rec[2];
	Record *last = NULL, *prev = NULL;
	*/
	Record r;
	Page measure;
	Record temp;
	ComparisonEngine ceng;
	//cout<<"\n Going to stop";
	//test t = this->tu;
	//while (tu.pipe->Remove (&r)==1) {
		/*prev = last;
		last = &rec[i%2];
		*/
	//	cout << "\n Oru";
		//db.Add(r);
	/*	if(pb.Append(&r) == 0)			
		{
			fb.AddPage(&pb, fb.GetLength()-1);
			pb.EmptyItOut();
			pb.Append(&r);
		}
		cout << "\n Going Out";*/
			
	//}
	//Store lastpage in file
//	cout << "\n AM ithe onw ";
	int pageno=0;
	int runcheck=0;
	int i=0;
	int j=0;
	vector<Record*> vec;
	vector<Cont*> con;
//	vec.reserve(100);
	//Whenever the page is full the pagebuffer stores that page in the filebuffer
	int cint =0;
	int acint =0;
	//cout<<"\nMax SIze is"<<vec.max_size();
	int pc=0;
	int nl=0;
	measure.EmptyItOut();
	while(input->Remove(&r)!= 0){
		//cout << "\n Are u here";
		Record *tu;
		tu  = new Record;
		tu->Copy(&r);
//                if(strcmp(filename,"leftfile.bin") == 0)
//                        r.Print(&l_sch);
		if(measure.Append(&r) == 0)			
		{
			pc++;
			measure.EmptyItOut();
			runcheck++;
			if(runcheck== runl){	
		        Cont *tt=new Cont;
			tt->current=nl;
			runcheck=0;
                        if(strcmp(filename,"leftfile.bin") == 0)
                                sort(vec.begin(),vec.end(),Compl);
                        else if (strcmp(filename,"rightfile.bin") == 0)
                            sort(vec.begin(),vec.end(),Compr);
                        else
                            sort(vec.begin(),vec.end(),Comp);
			int size = int(vec.size());
			int sa=0,ka=0;
			for(i=size-1;i>=0;i--){
				temp.Copy(vec[i]);
				vec.pop_back();
				int check=pb.Append(&temp);				
				if(check==0)			
				{
					if(fb.GetLength()==0){
					fb.AddPage(&pb,0);
					pageno=1;
					}
					else{	
						fb.AddPage(&pb, pageno);
						pageno++;
						
					}
				pb.EmptyItOut();
				pb.Append(&temp);
				}
				if(i==0){
					if(fb.GetLength()==0){
					fb.AddPage(&pb,0);
					pageno=1;
					}
					else{	
						fb.AddPage(&pb, pageno);
						pageno++;
					}
				tt->limits=pageno;
				nl=pageno;
				pb.EmptyItOut();
				}
			}
			con.push_back(tt);
			}
			measure.Append(&r);
		}
		vec.push_back(tu);
				
	}
//	cout<<"\nInsert finished\n";
        
	if(!vec.empty()){
		Cont *tt=new Cont;
		tt->current=pageno;
		pb.EmptyItOut();
                
		if(strcmp(filename,"leftfile.bin") == 0)
                                sort(vec.begin(),vec.end(),Compl);
                else if (strcmp(filename,"rightfile.bin") == 0)
                            sort(vec.begin(),vec.end(),Compr);
                        else
                            sort(vec.begin(),vec.end(),Comp);
		int ka=0,size = int(vec.size());
		//cout <<"\n Mass da " << size;		
		for(i=size-1;i>=0;i--){
       	        	temp.Copy(vec[i]);
//                        temp.Print(&l_sch);
			int check=pb.Append(&temp);
			if(check == 0)
                        {
                                //cout << "\nchecking if adding page inside \n";
				//cout<<"\nKas sd "<<ka;				
				//cout<<"File is : " <<fb.GetLength();
				if(fb.GetLength()==0){
//                                    cout << "File empty\n";
                               	fb.AddPage(&pb,0);
                         	pageno=1;
                       		}
                        	else{
//                                    cout<<"Extra pages\n";
                       	        	fb.AddPage(&pb, pageno);
                       		         pageno++;
                        	}
                       	pb.EmptyItOut();
			//cout << "\n and temp is : " << &temp;
                        pb.Append(&temp);
			//cout << "\n I is here : " << size-i;
			ka++;
        	        }
			else{ka++;}
			//vec.pop_back();
          	}
		//cout<<"\nKas "<<ka;
		tt->limits=pageno+1;
		con.push_back(tt);
	}
	fb.AddPage(&pb, pageno);
		
	//cout<<"\nSize is   "<<int(vec.size());
	
	//cout << endl << &pb << endl << pageno;
	//Store lastpage in file
	//cout << "\n Page Number is "<< pageno<<" most impo " << fb.GetLength();
	pb.EmptyItOut();
	merging(filename,con);
//        cout<<"\nBigQ shut\n";
	output->ShutDown();
//        cout << "Bigq over\n";
//	//for(
	//db = dbFile;*/
	/*if (prev && last) {
			if (ceng.Compare (prev, last, t->order) == 1) {
				err++;
			}
			if (t->write) {
				dbfile.Add (*prev);
			}
		}
		if (t->print) {
			last->Print (rel->schema ());
		}
		i++;
	}

	cout << " consumer: removed " << i << " recs from the pipe\n";

	if (t->write) {
			dbfile.Add (*last);
		if (last) {
		}
		cerr << " consumer: recs removed written out as heap DBFile at " << outfile << endl;
		dbfile.Close ();
	}
	cerr << " consumer: " << (i - err) << " recs out of " << i << " recs in sorted order \n";
	if (err) {
		cerr << " consumer: " <<  err << " recs failed sorted order test \n" << endl;
	}*/
	//cout<< "\n Tatatqtat";
	fb.Close();
        pthread_exit(NULL);
}

int BigQ::Comp(const void *left, const void *right)
{
	ComparisonEngine ceng;
	Record *x,*y;
	x = (Record *)left;
	y = (Record *)right;
	int a;
	a=ceng.Compare(x,y,sortin);
	if(a<=0)
		a=0;

	else 
		a=1;

	return a;
}

int BigQ::Compl(const void *left, const void *right)
{
	ComparisonEngine ceng;
	Record *x,*y;
	x = (Record *)left;
	y = (Record *)right;
	int a;
	a=ceng.Compare(x,y,sortl);
	if(a<=0)
		a=0;

	else 
		a=1;

	return a;
}

int BigQ::Compr(const void *left, const void *right)
{
	ComparisonEngine ceng;
	Record *x,*y;
	x = (Record *)left;
	y = (Record *)right;
	int a;
	a=ceng.Compare(x,y,sortr);
	if(a<=0)
		a=0;

	else 
		a=1;

	return a;
}


int BigQ::merging(char *a, vector <Cont*> pt){
	//fb.Open(0,a);
	//cout << "\n\nFile name is "<<a;
	Record stemp;
        OrderMaker *sortej;
        if(strcmp(filename,"leftfile.bin") == 0) {
            sortej = sortl;
        }
        else if (strcmp(filename,"rightfile.bin") == 0)
            sortej = sortr;
        else {
            sortej = sortin;
        }
            priority_queue <Cont *,vector<Cont *>,CompareForQ> pq(sortej);
        

	
	vector <Cont *> v;
	int i;
	int size = pt.size();
	Cont *tempo = new Cont;
	Cont *con=new Cont[size];
	int rep = 0;
        
        Attribute IA = {"int", Int};
        Attribute SA = {"string", String};
        Attribute DA = {"double", Double};

        Attribute l_att[] = {IA,SA,SA,IA,SA,DA,SA};

        Schema l_sch ("l_sch", 7, l_att);
        Page p1;
        Record r1;
//        fb.GetPage(&p1,0);
//        cout << "Record :\n"; 
//        while(p1.GetFirst(&r1)) {
//            r1.Print(&l_sch);
//        }
        
	for(i=0;i<size;i++){
		//cout << "\nRes "<<rep++;
		fb.GetPage(&con[i].p,pt[i]->current);
		con[i].current	= pt[i]->current;
		con[i].index	= i;
		con[i].limits	= pt[i]->limits; 
		con[i].p.GetFirst(&con[i].r);
		//cout << "\nRes "<<rep++ << " asdasd "<<con[i].current <<"   "<<con[i].limits;
                
		pq.push(&con[i]);
	}
	//cout << "\ndfs  is " << pq.size();
	/*if(seg*runl != no){
		cout << "\nYou are "<<seg * runl<<" anbd " << no <<" as "<< rem;
		fb.GetPage(&con[seg].p,seg*runl);
		con[seg].p.GetFirst(&con[seg].r);
		con[seg].current = seg*runl;
		con[seg].limits = seg*runl+rem;
		con[seg].index	= seg;
		pq.push(&con[seg]);
		int gh=0;
		while(!pq.empty())
			{
				//cout <<"\nHsta  "<<gh++;
				int tempind;
				tempo = pq.top();
				tempind=tempo->index;
				pq.pop();
				output->Insert(&tempo->r);
				if(con[tempind].p.GetFirst(&con[tempind].r)){
					//cout<<"\n Hji";
					pq.push(&con[tempind]);
				}
				else {
					cout <<"\n What hapes";
					con[tempind].current = con[tempind].current+1;
					if(con[tempind].current < con[tempind].limits){
						fb.GetPage(&con[tempind].p,con[tempind].current);
						if(con[tempind].p.GetFirst(&con[tempind].r)){
							pq.push(&con[tempind]);
						}
					}
				}
		
			}
		}
		
		else{
			int gh=0;			
			cout << "\nJhis";*/
		int count=0;
                bool flag1 = true;
                
                
        
        
		while(!pq.empty()){
				//cout << "\n dfasd is "<<gh++;				
			tempo = pq.top();
			int tempind=tempo->index;
			pq.pop();
                        
//                        if(flag1) {
//                            cout<<"\n Going to Inser first rec in outpipe\n";
//                            tempo->r.Print(&l_sch);
//                                    flag1=false;
//                        }
			output->Insert(&tempo->r);
			if(con[tempind].p.GetFirst(&con[tempind].r))
				pq.push(&con[tempind]);
			else{
				con[tempind].current = con[tempind].current+1;
				if(con[tempind].current < con[tempind].limits){
					fb.GetPage(&con[tempind].p,con[tempind].current);
					con[tempind].p.GetFirst(&con[tempind].r);
					pq.push(&con[tempind]);
					}
				}						
			}

	/*	while(con->p.GetFirst(&stemp)==1){
			output->Insert(&stemp);
		}*/
	//}
	//fb.Close();
}
/*	char *val1, *val2;

	char *left_bits = left->GetBits();
	char *right_bits = right->GetBits();

	for (int i = 0; i < sort->numAtts; i++) {
		val1 = left_bits + ((int *) left_bits)[sort->whichAtts[i] + 1];
		val2 = right_bits + ((int *) right_bits)[sort->whichAtts[i] + 1];
	
		// these are used to store the two operands, depending on their type
		int val1Int, val2Int;
		double val1Double, val2Double;
		
		// now check the type and do the comparison
		switch (sort->whichTypes[i]) {
	
			// first case: we are dealing with integers
			case Int:
	
			// cast the two bit strings to ints
			val1Int = *((int *) val1);
			val2Int = *((int *) val2);
	
			// and do the comparison
			if (val1Int < val2Int) 
				return -1;
			else if (val1Int > val2Int)
				return 1;
	
			break;
	
	
			// second case: dealing with doubles
			case Double:
	
			// cast the two bit strings to doubles
			val1Double = *((double *) val1);
			val2Double = *((double *) val2);
	
			// and do the comparison
			if (val1Double < val2Double)
				return -1;
			else if (val1Double > val2Double)
				return 1;
	
			break;
	
	
			// last case: dealing with strings
			default:
			int sc = strcmp (val1, val2);
			if (sc != 0)
				return sc;

			break;
	
		}
	}

	return 0;
}*/

BigQ::~BigQ () {
}
