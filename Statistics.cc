#include "Statistics.h"
#include <cassert>
#include <string>
#include <iostream>
#include <cstdlib>
#include <vector>
#include <set>
#include <tr1/unordered_set>
#include <algorithm>
#include <fstream>
using namespace std;

Statistics::Statistics() :
  rels(), extantAttrs(), mergedRelations()
{
}

Statistics::Statistics(Statistics &copyMe __attribute__ ((__unused__))) :
  rels(), extantAttrs(), mergedRelations()
{
    //Just propogates a deep copy in this system
}

Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples)
{
  //Constructor of the string is assigned and a set is created  
  string str(relName);
  const RelInf newRelation (numTuples, str);
  rels[str] = newRelation;
  rels.insert( make_pair(str,newRelation));
  rels[str].print();

}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
  
  string const rel(relName);
  string const att(attName);
  if (numDistincts == -1)
    {
      rels[rel].AddAtt(att,rels[rel].NumTuples());
      extantAttrs.insert(make_pair(att,rel));
    }
  else
    {
      rels[rel].AddAtt(att,numDistincts);
      extantAttrs.insert(make_pair(att,rel));
    }
  
}

void Statistics::CopyRel(char *oldName, char *newName)
{
  string oldN(oldName);
  string newN(newName);
  map < string, tupleCount > const oldAttrs = rels[oldN].GetAtts();

  RelInf newR(rels[oldN].NumTuples(), oldN);

  map < string, tupleCount >::const_iterator it;
  for (it = oldAttrs.begin(); it != oldAttrs.end(); it++ )
    {
      string newAttrName(newN+"."+(*it).first);
      //cout << "Changing from " << newAttrName << newAttrName.size() << " to " << newN << endl;
      newR.AddAtt(newAttrName, (*it).second);
      extantAttrs[newAttrName] = newN; 
    }

  rels[newN] = newR; // put new relation in
  newR.print();
}

void Statistics::Read(char *fromWhere)
{
  cout << endl;
  ifstream statFile(fromWhere);

  if (!statFile.good())
    {
      return;
    }

  unsigned iters;
  statFile >> iters;
  for(unsigned i = 0; i < iters; i++)
    {
      string relation;
      RelInf RI;
      statFile >> relation;
      statFile >> RI;
      rels[relation] = RI;
    }
  statFile >> iters;

  for(unsigned i = 0; i < iters; i++)
    {
      string attr;
      string relation;
      statFile >> attr >> relation;
      extantAttrs[attr] = relation;
    }
  statFile >> iters;
  for(unsigned i = 0; i < iters; i++)
    {
      string relation;
      string mergedrelation;
      statFile >> relation >> mergedrelation;
      mergedRelations[relation] = mergedrelation;
    }
}

void Statistics::Write(char *fromWhere)
{
  using std::ofstream;
  ofstream statFile(fromWhere);

  statFile << rels.size() << endl;
  {
    map < string, RelInf >::iterator it;
    for (it = rels.begin(); it != rels.end(); it++ )
      {
        statFile << (*it).first << endl << (*it).second << endl;
      }
  }
  statFile << extantAttrs.size() << endl;
  {
    map < string, string>::iterator it;
    for (it = extantAttrs.begin(); it != extantAttrs.end(); it++ )
      {
        statFile << (*it).first << endl << (*it).second << endl;
      }
  }
  statFile << mergedRelations.size() << endl;
  {
    map < string, string>::const_iterator it;
    for (it = mergedRelations.begin(); it != mergedRelations.end(); it++ )
      {
        statFile << (*it).first << endl << (*it).second << endl;
      }
  }

  statFile.close();
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
  JoinChecker(relNames, numToJoin);
  vector <string> attrs = ParseTreeChecker(parseTree);
  // if we actually return, the parse tree is good
  double estimate = 0;
  if (parseTree == 0 and  numToJoin <=2)
    {
      double accumulator = 1.0l;
      for (signed i = 0; i < numToJoin; i++)
        {
          string rel(relNames[i]);
          accumulator *= rels[rel].NumTuples();
        }
      estimate = accumulator;
    }
  else
    {
      estimate = CalculateEstimate(parseTree);
    }
  // make new name for joined relation.
  string newRelation;
  if(HasJoin(parseTree))
    {
      for (signed i = 0; i < numToJoin; i++)
        {
          string rel(relNames[i]);
          newRelation += rel;
        }
      //cout << "new relation is " << newRelation << endl;
      // new map, to have both relations merged into it.
      RelInf merged(estimate,newRelation); // new relation with estimated

      for (int i = 0; i < numToJoin ; i++)
        {
          merged.CopyAtts(rels[relNames[i]]);
        }
     // cout << "printing merged relations " << endl << endl;
      merged.print();
      rels[newRelation] = merged;

      { // get rid of information about old relations
      vector<string> attrsInParseTree  = ParseTreeChecker(parseTree);
      // create a set of old relations to steal attributes from.
      set<string> oldRels;
      for(vector<string>::iterator it = attrsInParseTree.begin(); it < attrsInParseTree.end(); it++)
        {
          oldRels.insert(extantAttrs[(*it)]);
        }

      for(set<string>::const_iterator it = oldRels.begin(); it != oldRels.end(); it++)
        {
          merged.CopyAtts(rels[*it]);
          rels.erase((*it));
        }
      for (int i = 0; i < numToJoin ; i++)
        {
          rels.erase(relNames[i]); //
          mergedRelations[relNames[i]] = newRelation;
        }
      }

      map<string, tupleCount> mergedAtts = merged.GetAtts();

      map < string, tupleCount>::const_iterator it;
      for (it = mergedAtts.begin(); it != mergedAtts.end(); it++ )
        {
          extantAttrs[(*it).first] = newRelation;
         // cout << (*it).first << "now belongs to " << newRelation << endl;
        }
    }

  cout << endl << endl << "Calculated Answer is: " << estimate;
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
  if (parseTree == 0 and  numToJoin <= 2)
    {
      double accumulator = 1.0l;
      for (signed i = 0; i < numToJoin; i++)
        {
          string rel(relNames[i]);
          accumulator *= rels[rel].NumTuples();
        }
      return accumulator;
    }

  JoinChecker(relNames, numToJoin);
  vector <string> attrs = ParseTreeChecker(parseTree);
  // if we actually return, the parse tree is good
  
  cout <<"\nNumber of Attributes are: "<< attrs.size() << endl;
  cout <<"Attribute Names :\n";
  for (unsigned int i = 0; i < attrs.size(); i++)
    {
      cout << attrs[i] << endl;
    }
  double result = CalculateEstimate(parseTree);
  cout << "Calculated Estimate " << result << endl;
  return result;
}

void Statistics :: Check (struct AndList *parseTree, char *relNames[], int numToJoin)
{
  JoinChecker(relNames, numToJoin);
  ParseTreeChecker(parseTree);
  // if we actually return, the parse tree is good
  return;
}

void Statistics :: JoinChecker(char *relNames[], int numToJoin)
{
 for (int i = 0; i < numToJoin; i++)
    {
      string rel(relNames[i]);
      // try for a singleton relation &
      // try for a merged relation
      //cout << "looking for rel " << rel << endl;
      //cout << "single rel count" << rels.count(rel) << endl;
      //cout << "merged rel count" << mergedRelations.count(rel) << endl;
      if (0 == rels.count(rel) and 0 == mergedRelations.count(rel))
        {
          //cout << "relation " << rel << " not found in internal relation tracker" << endl;
          exit(-1);
        }
    }
 // cout << "found all relations, .... now checking for all attrs in parsetree" << endl;
}

// returns a vector of the attrs
vector<string> Statistics :: ParseTreeChecker(struct AndList *pAnd)
{
  vector < string > attrs;
  attrs.reserve(100);
  while (pAnd)
    {
      struct OrList *pOr = pAnd->left;
      while (pOr)
        {
          struct ComparisonOp *pCom = pOr->left;
          if (pCom!=NULL)
            {
              {
                struct Operand *pOperand = pCom->left;
                if(pOperand!=NULL && (NAME == pOperand->code))
                  {
                    // check left operand
                    string attr(pOperand->value);
                    /*if (0 == extantAttrs.count(attr))
                      {
                        cerr << "left operand attribute \"" << attr << "\" not found" << endl;
                        assert(0 != extantAttrs.count(attr));
                        exit(-1);
                      }*/
                    attrs.push_back(attr);
                  }
              }
              {
                struct Operand *pOperand = pCom->right;
                if(pOperand!=NULL && (NAME == pOperand->code))
                  {
                    // check right operand
                    string attr(pOperand->value);
                    if (0 == extantAttrs.count(attr))
                      {
                        cerr << "right operand attribute \"" << attr << "\" not found" << endl;
                        exit(-1);
                      }
                    attrs.push_back(attr);
                  }
              }
            }
          pOr = pOr->rightOr;
        }
      pAnd = pAnd->rightAnd;
    }
  return attrs; // return by copy
}

double Statistics :: CalculateEstimate(AndList *pAnd)
{
  double result = 1.0l;
  bool seenJoin = false;
  double selectOnlySize = 0.0l;
  while (pAnd)
    {
      OrList *pOr = pAnd->left;
      bool independentORs = true; // assume independence
      bool singleOR = false;
     // cout << "singleOr is " << singleOR << endl;
      { // but check
        set <string> ors;
        unsigned count = 0;
        while (pOr) // traverse with counter.
          {
            ComparisonOp *pCom = pOr->left;
            if (pCom!=NULL)
              {
               // cout << count;
                count++;
                string attr(pOr->left->left->value);
                //cout << "orattr is " << attr << endl;
                //cout << "or.size is " << ors.size() << endl;
                ors.insert(attr);
              }
            pOr = pOr->rightOr;
          }
        if (ors.size() != count)
          {
            independentORs = false;
        }
        if (count == 1)
          {
            independentORs = false; 
            //cout << "singleOr is " << singleOR << endl;
            singleOR = true; 
          }
        
      }
      //cout << "singleOr is " << singleOR << endl;
      pOr = pAnd->left; // reset pointer
      double tempOrValue = 0.0l; // each or is calculated separately, and then multiplied in at the end.
      if(independentORs)
        {
          tempOrValue = 1.0l;
      }
      while (pOr)
        {
          struct ComparisonOp *pCom = pOr->left;
          if (pCom!=NULL)
            {
              
              Operand *lOperand = pCom->left;
              Operand *rOperand = pCom->right;
              switch(pCom->code)
                {
                case EQUALS: // maybe selection or maybe join
                  {
                    if ((lOperand != 0 and (NAME == lOperand->code)) and
                        (rOperand != 0 and (NAME == rOperand->code)))
                      {// this is a join, because both the left and right are attribute names
                       // cout << endl << "join case estimation" << endl << endl;
                        seenJoin = true;
                        string const lattr(lOperand->value);
                        string const rattr(rOperand->value);
                        // look up which relation l attr is in
                        string const lrel = extantAttrs[lattr];
                        // get size of l relation
                        tupleCount const lRelSize = rels[lrel].NumTuples();
                        // get number of Distinct values of L attr
                        int const lDistinct = rels[lrel].GetDistinct(lattr);
                        // look up which relation r attr is in
                        string const rrel = extantAttrs[rattr];
                        // get size of r relation
                        tupleCount const rRelSize = rels[rrel].NumTuples();
                        // get number of Distinct values of R attr
                        int const rDistinct = rels[rrel].GetDistinct(rattr);

                       
                        double numerator   = lRelSize * rRelSize;
                        double denominator = max(lDistinct,rDistinct);

                        tempOrValue += (numerator/denominator);
                       }
                    else
                      { // this is a selection // maybe fall through?
                        Operand *opnd = 0;
                        Operand *constant = 0;
                        if (NAME == lOperand->code)
                          {opnd = lOperand; constant = rOperand; }
                        else if (NAME == rOperand->code)
                          {opnd = rOperand; constant = lOperand;}
                     //   assert(0 != opnd); // something was assigned
                      //  assert(0 != constant); // something was assigned

                        string const attr(opnd->value);
                        string const relation = extantAttrs[attr];
                        tupleCount const distinct = rels[relation].GetDistinct(attr);
                        if (singleOR)
                          {
                            double const calculation = (1.0l/distinct);// (numerator/denominator);

                            tempOrValue += calculation;
                          }
                        else
                          {
                            if(independentORs) // independent ORs
                              {
                                double const calculation = (1.0l - (1.0l/distinct));
                                tempOrValue *= calculation;
                              }
                            else // dependent ORs
                              {
                                // else
                                {
                                  double const calculation = (1.0l/distinct);
                                  tempOrValue += calculation;
                                }
                              }
                          }
                      }
                    break;
                  }
                case LESS_THAN: // selection
                  //break;
                case GREATER_THAN: // selection
                  // break;
                  // we are in a selection now.
                  // so either of our operands could be a literal value rather than an attribute
                  Operand *opnd = 0;
                  Operand *constant = 0;
                  if (NAME == lOperand->code)
                    {opnd = lOperand; constant = rOperand; }
                  else if (NAME == rOperand->code)
                    {opnd = rOperand; constant = lOperand;}
                  assert(0 != opnd); // something was assigned
                  assert(0 != constant); // something was assigned

                  string const attr(opnd->value);
                  string const relation = extantAttrs[attr];

                  if(independentORs) // independent ORs
                    {
                      double const calculation = 1.0l - (1.0l)/(3.0l);;
                      tempOrValue *= calculation;
                    }
                  else // dependent ORs
                    {
                      double const calculation = (1.0l)/(3.0l);
                      tempOrValue += calculation;
                    }
                  break;
                }
              if (!seenJoin)
                {
                  Operand *opnd = 0;
                  if (NAME == lOperand->code)
                    {opnd = lOperand;}
                  else if (NAME == rOperand->code)
                    {opnd = rOperand;}
                  string const attr(opnd->value);
                  string const relation = extantAttrs[attr];
                  tupleCount const relationSize = rels[relation].NumTuples();
                  selectOnlySize = relationSize;
                }
              {
                struct Operand *pOperand = pCom->left;
                if(pOperand!=NULL and (NAME == pOperand->code))
                  {
                    // check left operand
                    string attr(pOperand->value);
                    if (extantAttrs.count(attr) == 0)
                      {
                        cerr << "estimate left operand attribute \"" << attr << "\" not found" << endl;
                       // assert(0 != extantAttrs.count(attr));
                        exit(-1);
                      }
                  }
              }
              // operator
              {
                struct Operand *pOperand = pCom->right;
                if(pOperand!=NULL and (NAME == pOperand->code))
                  {
                    // check right operand
                    string attr(pOperand->value);
                    if (extantAttrs.count(attr) == 0)
                      {
                        cerr << "estimate right operand attribute \"" << attr << "\" not found" << endl;
                        assert(0 != extantAttrs.count(attr));
                        exit(-1);
                      }
                  }
              }
            }
          pOr = pOr->rightOr; // go to next or
        }
      //cout << "putting ors into and estimate" << endl;
      if (independentORs)
        {
        //  cout << "before, result was " << result << endl;
          result *= (1 - tempOrValue);
         // cout << "after, result was " << result << endl;
        }
      else
        {
          result *= tempOrValue;
          //cout << "Number of tempOrs " << result << endl;
        }
      pAnd = pAnd->rightAnd; // go to next and
    }
  if (!seenJoin)
    {
      result *= selectOnlySize;
    }
  return result;
}

bool Statistics :: HasJoin(AndList *pAnd)
{
  double result = 1.0l;
  bool seenJoin = false;
  double selectOnlySize = 0.0l;
  while (pAnd)
    {
      OrList *pOr = pAnd->left;
      bool independentORs = true; // assume independence
      bool singleOR = false;
   //   cout << "singleOr is " << singleOR << endl;
      { // but check
        set <string> ors;
        unsigned count = 0;
        while (pOr) // traverse with counter.
          {
            ComparisonOp *pCom = pOr->left;
            if (pCom!=NULL)
              {
              //  cout << count;
                count++;
                string attr(pOr->left->left->value);
               // cout << "orattr is " << attr << endl;
                //cout << "or.size is " << ors.size() << endl;
                ors.insert(attr);
              }
            pOr = pOr->rightOr;
          }
        if (ors.size() != count)
          {
            independentORs = false;
          }
        if (count == 1)
          {
            independentORs = false;
            singleOR = true; 
            
        }
       }
      pOr = pAnd->left; // reset pointer
      double tempOrValue = 0.0l; // each or is calculated separately, and then multiplied in at the end.
      if(independentORs)
        {tempOrValue = 1.0l;}
      while (pOr)
        {
          struct ComparisonOp *pCom = pOr->left;
          if (pCom!=NULL)
            {
              Operand *lOperand = pCom->left;
              Operand *rOperand = pCom->right;
              switch(pCom->code)
                {
                case EQUALS: // maybe selection or maybe join
                  {
                    if ((lOperand != 0 and (NAME == lOperand->code)) and
                        (rOperand != 0 and (NAME == rOperand->code)))
                      {// this is a join, because both the left and right are attribute names
                       // cout << endl << "join case estimation" << endl << endl;
                        seenJoin = true;
                        string const lattr(lOperand->value);
                        string const rattr(rOperand->value);
                        // look up which relation l attr is in
                        string const lrel = extantAttrs[lattr];
                        // get size of l relation
                        tupleCount const lRelSize = rels[lrel].NumTuples();
                        // get number of Distinct values of L attr
                        int const lDistinct = rels[lrel].GetDistinct(lattr);
                        // look up which relation r attr is in
                        string const rrel = extantAttrs[rattr];
                        // get size of r relation
                        tupleCount const rRelSize = rels[rrel].NumTuples();
                        // get number of Distinct values of R attr
                        int const rDistinct = rels[rrel].GetDistinct(rattr);

                       
                        double numerator   = lRelSize * rRelSize;
                        double denominator = max(lDistinct,rDistinct);

                        tempOrValue += (numerator/denominator);
                      }
                    else
                      { // this is a selection // maybe fall through?
                        Operand *opnd = 0;
                        Operand *constant = 0;
                        if (NAME == lOperand->code)
                          {opnd = lOperand; constant = rOperand; }
                        else if (NAME == rOperand->code)
                          {opnd = rOperand; constant = lOperand;}
                     
                        string const attr(opnd->value);
                        string const relation = extantAttrs[attr];
                        tupleCount const distinct = rels[relation].GetDistinct(attr);
                        //cout << "singleOr is " << singleOR << endl;
                        if (singleOR)
                          {
                            double const calculation = (1.0l/distinct);// (numerator/denominator);

                          //  cout << "single value is " << calculation << endl;
                            tempOrValue += calculation;
                          }
                        else
                          {
                            if(independentORs) // independent ORs
                              {
                                double const calculation = (1.0l - (1.0l/distinct));
                                //cout << "indep, value is " << calculation << endl;
                                tempOrValue *= calculation;
                              }
                            else // dependent ORs
                              {
                                // else
                                {
                                  double const calculation = (1.0l/distinct);
                                 // cout << "dep, value is " << calculation << endl;
                                  tempOrValue += calculation;
                                }
                              }
                          }
                      }
                    break;
                  }
                case LESS_THAN: // selection
                  //break;
                case GREATER_THAN: // selection
                  // break;
                  //cout << "not equal selection fall through" << endl;
                  // we are in a selection now.
                  // so either of our operands could be a literal value rather than an attribute
                  Operand *opnd = 0;
                  Operand *constant = 0;
                  if (NAME == lOperand->code)
                    {opnd = lOperand; constant = rOperand; }
                  else if (NAME == rOperand->code)
                    {opnd = rOperand; constant = lOperand;}
               
                  string const attr(opnd->value);
                  string const relation = extantAttrs[attr];

                  if(independentORs) // independent ORs
                    {
                      double const calculation = 1.0l - (1.0l)/(3.0l);;
                      //cout << "indep, value is " << calculation << endl;
                      tempOrValue *= calculation;
                    }
                  else // dependent ORs
                    {
                      double const calculation = (1.0l)/(3.0l);
                     // cout << "dep, value is " << calculation << endl;
                      tempOrValue += calculation;
                    }
                  break;
                }
              if (!seenJoin)
                {
                  Operand *opnd = 0;
                  if (NAME == lOperand->code)
                    {opnd = lOperand;}
                  else if (NAME == rOperand->code)
                    {opnd = rOperand;}
                  string const attr(opnd->value);
                  string const relation = extantAttrs[attr];
                  tupleCount const relationSize = rels[relation].NumTuples();
                  selectOnlySize = relationSize;
                }
              {
                struct Operand *pOperand = pCom->left;
                if(pOperand!=NULL and (NAME == pOperand->code))
                  {
                    // check left operand
                    string attr(pOperand->value);
                    if (extantAttrs.count(attr) == 0)
                      {
                        cerr << "operand attribute \"" << attr << "\" not found" << endl;
                        assert(0 != extantAttrs.count(attr));
                        exit(-1);
                      }
                  }
              }
              // operator
              {
                struct Operand *pOperand = pCom->right;
                if(pOperand!=NULL and (NAME == pOperand->code))
                  {
                    // check right operand
                    string attr(pOperand->value);
                    if (extantAttrs.count(attr) == 0)
                      {
                        cerr << "operand attribute \"" << attr << "\" not found" << endl;
                        exit(-1);
                      }
                  }
              }
            }
          pOr = pOr->rightOr; // go to next or
        }
      if (independentORs)
        {
          result *= (1 - tempOrValue);
        }
      else
        {
          result *= tempOrValue;
        }
      pAnd = pAnd->rightAnd; // go to next and
    }
  if (!seenJoin)
    {
      result *= selectOnlySize;
    }
  return seenJoin;
}