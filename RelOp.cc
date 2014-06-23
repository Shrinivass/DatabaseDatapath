#include "RelOp.h"
#include "BigQ.h"
#include "SortedDBFile.h"
#include <alloca.h>
#include <iostream>
#include <sstream>
#include <string>

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    inF = &inFile;
    outP = &outPipe;
    cnf = &selOp;
    lit = &literal;
    pthread_create(&SelectFileThread, NULL, &SelectFile::thread_starter, this);
}

void * SelectFile::thread_starter(void *context) {
    return reinterpret_cast<SelectFile*> (context)->WorkerThread();
}

void * SelectFile::WorkerThread(void) {
    DBFile & inFile = *inF;
    Pipe & outPipe = *outP;
    CNF & selOp = *cnf;
    Record & literal = *lit;
    int counter = 0;
    int pipeSize = outPipe.GetPipeSize();
    Record temp;
    inFile.MoveFirst();
    while (inFile.GetNext(temp, selOp, literal) == 1) {
        counter += 1;

        outPipe.Insert(&temp);

    }
    cout << endl << " selected " << counter << " recs \n";
    outPipe.ShutDown();
    pthread_exit(NULL);
}

void SelectFile::WaitUntilDone() {
    pthread_join(SelectFileThread, NULL);
}

void * SelectPipe::thread_starter(void *context) {
    return reinterpret_cast<SelectPipe*> (context)->WorkerThread();
}

void * SelectPipe::WorkerThread(void) {
    Pipe & inPipe = *in;
    Pipe & outPipe = *out;
    CNF & selOp = *cnf;
    Record & literal = *lit;
    int counter = 0;
    Record temp;

    ComparisonEngine comp;
    while (inPipe.Remove(&temp)) {
        if (comp.Compare(&temp, &literal, &selOp) == 1) {
            counter++;
            outPipe.Insert(&temp);
        }
    }
    cout << endl << " selected " << counter << " recs \n";

    outPipe.ShutDown();
    pthread_exit(NULL);
}

void SelectPipe::WaitUntilDone() {
    pthread_join(SelectPipeThread, NULL);
}

void * Sum::thread_starter(void *context) {
    return reinterpret_cast<Sum*> (context)->WorkerThread();
}

void * Sum::WorkerThread(void) {
    cout << "Sum thread started" << endl;
    Pipe &inPipe = *in;
    Pipe &outPipe = *out;
    Function &computeMe = *fn;
    Record temp;
    Type retType = Int;
    unsigned int counter = 0;
    int intresult = 0;
    double doubleresult = 0.0;
    while (inPipe.Remove(&temp) == 1) {
        counter++;
        int tr = 0;
        double td = 0.0;
        retType = computeMe.Apply(temp, tr, td);
        intresult += tr;
        doubleresult += td;
    }
    {
        Record ret;
        stringstream ss;
        Attribute attr;
        attr.name = "SUM";
        if (retType == 0) {
            attr.myType = Int;
            ss << intresult << "|";
        } else if (retType == 1) {
            attr.myType = Double;
            ss << doubleresult << "|";
        }
        Schema retSchema("out_schema", 1, &attr);
        ret.ComposeRecord(&retSchema, ss.str().c_str());
        outPipe.Insert(&ret);
    }
    outPipe.ShutDown();
    pthread_exit(NULL);
}

void * GroupBy::thread_starter(void *context) {
    return reinterpret_cast<GroupBy*> (context)->WorkerThread();
}

void * GroupBy::WorkerThread(void) {
    cout << "GroupBy thread started" << endl;
    Pipe &inPipe = *in;
    Pipe &outPipe = *out;
    Attribute nation[] = {
        {"int", Int},
        {"string", String},
        {"string", String},
        {"int", Int}};
    Schema nat_sch("nat_sch", 4, nation);
    Record r;
    OrderMaker & compare = *comp;
    Function &computeMe = *fn;
    Pipe sortedOutput(runLength);
    BigQ sorter(inPipe, sortedOutput, compare, runLength);
    Record recs[2];

    Type retType = Int;
    int intresult = 0;
    double doubleresult = 0.0;
    unsigned int counter = 0;

    if (sortedOutput.Remove(&recs[1]) == 1) {
        int tr;
        double td;
        retType = computeMe.Apply(recs[1], tr, td);
        intresult += tr;
        doubleresult += td;
        counter++;
    }
    unsigned int i = 0;
    ComparisonEngine ceng;
    while (sortedOutput.Remove(&recs[i % 2]) == 1) {
        if (ceng.Compare(&recs[i % 2], &recs[(i + 1) % 2], &compare) == 0) {
            int tr;
            double td;
            computeMe.Apply(recs[i % 2], tr, td);
            intresult += tr;
            doubleresult += td;
        } else {
            WriteRecordOut(recs[(i + 1) % 2], retType, intresult, doubleresult);
            i++;
        }
    }
    WriteRecordOut(recs[i % 2], retType, intresult, doubleresult);
    i++;

    outPipe.ShutDown();
    pthread_exit(NULL);
}

void GroupBy::WriteRecordOut(Record & rec, Type const retType, int & intresult, double & doubleresult) {
    {
        Record ret;
        stringstream ss;
        Attribute attr;
        attr.name = "SUM";
        if (retType == Int) {
            attr.myType = Int;
            ss << intresult << "|";
        } else if (retType == Double) {
            attr.myType = Double;
            ss << doubleresult << "|";
        }
        Schema retSchema("out_schema", 1, &attr);
        ret.ComposeRecord(&retSchema, ss.str().c_str());
        int RightNumAtts = comp->GetNumAtts();
        int numAttsToKeep = 1 + RightNumAtts;

        int * attsToKeep = (int *) alloca(sizeof (int) * numAttsToKeep);
        {
            int curEl = 0;
            attsToKeep[0] = 0;
            curEl++;

            for (int i = 0; i < RightNumAtts; i++) {
                attsToKeep[curEl] = (comp->GetWhichAtts())[i];
                curEl++;
            }
        }
        Record newret;
        newret.MergeRecords(&ret, &rec, 1, rec.GetNumAtts(),
                attsToKeep, numAttsToKeep, 1);
        Pipe &outPipe = *out;
        outPipe.Insert(&newret);
    }
    intresult = 0;
    doubleresult = 0.0;
}

void * Project::thread_starter(void *context) {
    return reinterpret_cast<Project*> (context)->WorkerThread();
}

void * Project::WorkerThread(void) {
    Pipe &inPipe = *in;
    Pipe &outPipe = *out;
    Record temp;
    unsigned int counter = 0;
    while (inPipe.Remove(&temp) == 1) {
        counter++;
        temp.Project(atts, numAttsOut, numAttsIn);
        outPipe.Insert(&temp);
    }
    outPipe.ShutDown();
    pthread_exit(NULL);
}

void * Join::thread_starter(void *context) {
    return reinterpret_cast<Join*> (context)->WorkerThread();
}

void * Join::WorkerThread(void) {
    Pipe& inPipeL = *inL;
    Pipe& inPipeR = *inR;
    Pipe& outPipe = *out;
    CNF& selOp = *cnf;
    unsigned int counterL = 0;
    unsigned int counterR = 0;
    unsigned int counterOut = 0;
    OrderMaker sortOrderL;
    OrderMaker sortOrderR;
    bool validOrderMaker = (0 < selOp.GetSortOrders(sortOrderL, sortOrderR));
    if (validOrderMaker) {
        {
            Pipe outPipeL(runLength);
            Pipe outPipeR(runLength);
            BigQ Left(inPipeL, outPipeL, sortOrderL, runLength, "leftfile.bin");
            BigQ Right(inPipeR, outPipeR, sortOrderR, runLength, "rightfile.bin");
            Record LeftRecord;
            Record RightRecord;
            unsigned int counter = 0;
            outPipeL.Remove(&LeftRecord);
            outPipeR.Remove(&RightRecord);

            const int LeftNumAtts = LeftRecord.GetNumAtts();
            const int RightNumAtts = RightRecord.GetNumAtts();
            const int NumAttsTotal = LeftNumAtts + RightNumAtts;

            int * attsToKeep = (int *) alloca(sizeof (int) * NumAttsTotal);
            {
                int curEl = 0;
                for (int i = 0; i < LeftNumAtts; i++)
                    attsToKeep[curEl++] = i;
                for (int i = 0; i < RightNumAtts; i++)
                    attsToKeep[curEl++] = i;
            }
            ComparisonEngine ceng;

            vector<Record> LeftBuffer;
            LeftBuffer.reserve(1000);
            vector<Record> RightBuffer;
            RightBuffer.reserve(1000);
            Record MergedRecord;

            do {
                do {
                    if (ceng.Compare(&LeftRecord, &sortOrderL, &RightRecord, &sortOrderR) > 0) {
                        do {
                            counter++;
                            counterR++;
                            if (outPipeR.Remove(&RightRecord) == 0) {
                                RightRecord.SetNull();
                                break;
                            }
                        } while (ceng.Compare(&LeftRecord, &sortOrderL, &RightRecord, &sortOrderR) > 0);
                    }
                    if (ceng.Compare(&LeftRecord, &sortOrderL, &RightRecord, &sortOrderR) < 0) {
                        do {
                            counter++;
                            counterL++;
                            if (outPipeL.Remove(&LeftRecord) == 0) {
                                LeftRecord.SetNull();
                                break;
                            }
                        } while (ceng.Compare(&LeftRecord, &sortOrderL, &RightRecord, &sortOrderR) < 0);
                    }
                } while (ceng.Compare(&LeftRecord, &sortOrderL, &RightRecord, &sortOrderR) != 0);
                FillBuffer(LeftRecord, LeftBuffer, outPipeL, sortOrderL);
                FillBuffer(RightRecord, RightBuffer, outPipeR, sortOrderR);

                unsigned int lSize = LeftBuffer.size();
                unsigned int rSize = RightBuffer.size();
                counterL += lSize;
                counterR += rSize;
                counterOut += (lSize * rSize);
                for (unsigned int i = 0; i < lSize; ++i) {
                    for (unsigned int j = 0; j < rSize; ++j) {
                        MergedRecord.MergeRecords(&LeftBuffer[i], &RightBuffer[j], LeftNumAtts, RightNumAtts, attsToKeep, NumAttsTotal, LeftNumAtts);
                        outPipe.Insert(&MergedRecord);
                    }
                }
                LeftBuffer.clear();
                RightBuffer.clear();

            } while (!LeftRecord.isNull() && !RightRecord.isNull());
        }
    }
    outPipe.ShutDown();
}

void Join::FillBuffer(Record & in, vector<Record> &buffer, Pipe & pipe, OrderMaker & sortOrder) {
    ComparisonEngine ceng;
    buffer.push_back(in);
    if (pipe.Remove(&in) == 0) {
        in.SetNull();
        return;
    }
    while (ceng.Compare(&buffer[0], &in, &sortOrder) == 0) {
        buffer.push_back(in);
        if (0 == pipe.Remove(&in)) {
            in.SetNull();
            return;
        }
    }
    return;
}

void * DuplicateRemoval::thread_starter(void *context) {
    return reinterpret_cast<DuplicateRemoval*> (context)->WorkerThread();
}

void * DuplicateRemoval::WorkerThread(void) {
    Pipe& inPipe = *in;
    Pipe& outPipe = *out;

    Pipe sortedOutput(runLength);
    BigQ sorter(inPipe, sortedOutput, compare, runLength);
    Record recs[2];
    unsigned int counter = 0;

    if (sortedOutput.Remove(&recs[1]) == 1) {
        Record copy;
        copy.Copy(&recs[1]);
        outPipe.Insert(&copy);
        counter++;
    }

    unsigned int i = 0;
    ComparisonEngine ceng;
    while (sortedOutput.Remove(&recs[i % 2]) == 1) {
        if (ceng.Compare(&recs[i % 2], &recs[(i + 1) % 2], &compare) != 0) {
            counter++;
            Record copy;
            copy.Copy(&recs[i % 2]);
            outPipe.Insert(&copy);
            i++;
        }
    }

    outPipe.ShutDown();
    pthread_exit(NULL);
}

void * WriteOut::thread_starter(void *context) {
    return reinterpret_cast<WriteOut*> (context)->WorkerThread();
}

void * WriteOut::WorkerThread(void) {
    Pipe& inPipe = *in;

    Record temp;
    while (inPipe.Remove(&temp) == 1) {
        ostringstream os;
        temp.Print(sch, os);
        fputs(os.str().c_str(), out);
    }
    pthread_exit(NULL);
}
