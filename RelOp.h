#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <string>
#include <vector>

class RelationalOp {
protected:
    int runLength;

public:
    RelationalOp() : runLength(100) {
    }
    virtual void WaitUntilDone() = 0;
    void Use_n_Pages(int n) {
        runLength = n;
    }
    int GetRunLength(void) {
        return runLength;
    }
};

class SelectFile : public RelationalOp { 
private:
    DBFile * inF;
    Pipe * outP;
    CNF * cnf;
    pthread_t SelectFileThread;
    Record *lit;

    static void *thread_starter(void *context);
    void * WorkerThread(void);
    SelectFile operator=(const SelectFile&);

public:
    SelectFile() : inF(0), outP(0), cnf(0), lit(0), SelectFileThread() {
    }
    void Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
    void WaitUntilDone();

};

class SelectPipe : public RelationalOp {
    Pipe * in;
    Pipe * out;
    CNF * cnf;
    pthread_t SelectPipeThread;
    Record * lit;
    static void *thread_starter(void *context);
    void * WorkerThread(void);

public:
    SelectPipe() : in(0), out(0), cnf(0), lit(0) {
    }
    void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
        out = &outPipe;
        cnf = &selOp;
        lit = &literal;
        pthread_create(&SelectPipeThread, NULL, &SelectPipe::thread_starter, this);
    }
    void WaitUntilDone();
};

class Project : public RelationalOp {
    Pipe * in;
    Pipe * out;
    int * atts;
    int numAttsIn;
    int numAttsOut;
    pthread_t ProjectThread;

    static void *thread_starter(void *context);
    void * WorkerThread(void);
public:
    Project() : in(0), out(0), atts(0), numAttsIn(0), numAttsOut(0), ProjectThread() {
    }
    void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
        in = &inPipe;
        out = &outPipe;
        atts = keepMe;
        numAttsIn = numAttsInput;
        numAttsOut = numAttsOutput;
        pthread_create(&ProjectThread, 0, &Project::thread_starter, this);
    }
    void WaitUntilDone() {
        pthread_join(ProjectThread, 0);
    }
};
class Join : public RelationalOp {
    Pipe * inL;
    Pipe * inR;
    Pipe * out;
    CNF * cnf;
    pthread_t JoinThread;


    static void *thread_starter(void *context);
    void * WorkerThread(void);
    void FillBuffer(Record &, vector<Record> &buffer, Pipe & pipe, OrderMaker & sortOrder);

public:
    Join() : inL(0), inR(0), out(0), cnf(0), JoinThread() {
    }
    void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal __attribute__((unused))) {
        inL = &inPipeL;
        inR = &inPipeR;
        out = &outPipe;
        cnf = &selOp;
        pthread_create(&JoinThread, NULL, &Join::thread_starter, this);
    }
    void WaitUntilDone() {
        pthread_join(JoinThread, NULL);
    }
};

class DuplicateRemoval : public RelationalOp {
    Pipe * in;
    Pipe * out;
    OrderMaker compare;
    pthread_t DuplicateRemovalThread;

    static void *thread_starter(void *context);
    void * WorkerThread(void);

public:
    DuplicateRemoval() : in(0), out(0), DuplicateRemovalThread() {
    }
    void Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
        in = &inPipe;
        out = &outPipe;
        compare = OrderMaker(&mySchema);
        pthread_create(&DuplicateRemovalThread, NULL, &DuplicateRemoval::thread_starter, this);
    }
    void WaitUntilDone() {
        pthread_join(DuplicateRemovalThread, NULL);
    }
};

class Sum : public RelationalOp {
    int integerResult;
    double FPResult;

    Pipe * in;
    Pipe * out;
    Function * fn;
    pthread_t SumThread;
    static void *thread_starter(void *context);
    void * WorkerThread(void);
public:

    Sum() : integerResult(0), FPResult(0), in(0), out(0), fn(0), SumThread() {
    }

    void Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
        in = &inPipe;
        out = &outPipe;
        fn = &computeMe;
        pthread_create(&SumThread, NULL, &Sum::thread_starter, this);
    }

    void WaitUntilDone() {
        pthread_join(SumThread, NULL);
    }
};

class GroupBy : public RelationalOp {
    Pipe * in;
    Pipe * out;
    OrderMaker * comp;
    Function * fn;

    pthread_t GroupByThread;
    static void *thread_starter(void *context);
    void * WorkerThread(void);
    GroupBy & operator=(const GroupBy&);
public:

    GroupBy() : in(0), out(0), comp(0), fn(0), GroupByThread() {
    }

    void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
        in = &inPipe;
        out = &outPipe;
        comp = &groupAtts;
        fn = &computeMe;
        pthread_create(&GroupByThread, NULL, &GroupBy::thread_starter, this);
    }

    void WaitUntilDone() {
        pthread_join(GroupByThread, NULL);
    }
    void WriteRecordOut(Record &, Type, int & intresult, double & doubleresult);
};

class WriteOut : public RelationalOp {
    Pipe * in;
    FILE * out;
    Schema * sch;
    pthread_t WriteOutThread;
    static void *thread_starter(void *context);
    void * WorkerThread(void);
public:

    WriteOut() : in(0), out(0), sch(0), WriteOutThread() {
    }

    void Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
        in = &inPipe;
        out = outFile;
        sch = &mySchema;
        pthread_create(&WriteOutThread, NULL, &WriteOut::thread_starter, this);
    }

    void WaitUntilDone() {
        pthread_join(WriteOutThread, NULL);
    }
};
#endif
