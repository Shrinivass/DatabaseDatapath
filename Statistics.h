#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <map>
#include <tr1/unordered_map>
#include <set>
#include <tr1/unordered_set>
#include <string>
#include <utility>
#include <vector>
#include <iostream>
using namespace std;
class RelInf
{
  typedef unsigned long long tupleCount;
  tupleCount numberOfTuples;
  map<string, tupleCount> attributeInformation;
  // Schema sch;
  string originalName;
 public:
  RelInf () : numberOfTuples(0), attributeInformation(), originalName() {}
  explicit RelInf (tupleCount tuples, string origNme) : numberOfTuples(tuples), attributeInformation(), /* sch(), */ originalName(origNme)
  {/* char * cat = "catalog"; sch = Schema(cat, origNme.c_str()); */}
  map<string, tupleCount> const GetAtts()
    {
      return attributeInformation;
    }
  void CopyAtts (RelInf const & otherRel)
  {
    attributeInformation.insert(otherRel.attributeInformation.begin(), otherRel.attributeInformation.end());
  }
  tupleCount GetDistinct (string attr)
  {
    return attributeInformation[attr];
  }
  void AddAtt (string attr, tupleCount numDistinct)
  {
    attributeInformation[attr] = numDistinct;
  }
  tupleCount NumTuples() {return numberOfTuples;}
  void print()
  {
    cout << numberOfTuples << endl;
    for (map<string, tupleCount>::iterator it=attributeInformation.begin() ; it != attributeInformation.end(); it++ )
      {cout << (*it).first << " => " << (*it).second << endl;}
  }

  friend ostream& operator<<(ostream& os, const RelInf & RI)
    {
      os << RI.numberOfTuples << endl;
      map<string, tupleCount>::const_iterator it;
      os << RI.attributeInformation.size() << endl;
      for (it = RI.attributeInformation.begin(); it != RI.attributeInformation.end(); it++ )
        {
          os << (*it).first << endl << (*it).second << endl;
        }
      return os;
    }

  friend istream& operator>>(istream& is, RelInf & RI)
    {
      cout << "reading relation" << endl;
      tupleCount numTups;
      is >> numTups;
      cout << "there are " << numTups << " tuples in this relation." << endl;
      RI.numberOfTuples = numTups;
      tupleCount numMappings;
      is >> numMappings;
      cout << "there are " << numMappings << " attributes in this relation." << endl;
      for (unsigned i = 0; i < numMappings; i++)
        {
          string attr;
          tupleCount distinct;
          is >> attr;
          is >> distinct;
          RI.attributeInformation[attr] = distinct;
        }
      return is;
    }
};

class Statistics
{
 private:
  typedef long long unsigned int tupleCount;

  map < string, RelInf > rels;
  map < string, string> extantAttrs; // if the attr exists, and what relation it is in. <k,v> is <attr, relation-attr-is-in>
  map < string, string> mergedRelations; // <k,v> is <original-relation-name, merged-relation-name>

  void Check (struct AndList *parseTree, char *relNames[], int numToJoin);
  void JoinChecker(char *relNames[], int numToJoin);
  vector<string> ParseTreeChecker(struct AndList *pAnd);
  double CalculateEstimate(struct AndList *pAnd);
  bool HasJoin(AndList *pAnd);
 public:
  Statistics();
  Statistics(Statistics &copyMe); // Performs deep copy
  ~Statistics();


  void AddRel(char *relName, int numTuples);
  void AddAtt(char *relName, char *attName, int numDistincts);
  void CopyRel(char *oldName, char *newName);

  void Read(char *fromWhere);
  void Write(char *fromWhere);

  void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
  double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

  string getAttrHomeTable(string a)
    {
      cout << "called get attr home table" << endl;
      // string a(attr);
      cout << a << a.size() << endl;
      cout << "found " << extantAttrs.count(a);
      if (1 == extantAttrs.count(a))
        {
          cout << "WTF" << endl;
          cout << extantAttrs[a] << endl;
          string tbl(extantAttrs[a]);
          cout << tbl << endl;
          return tbl;
        }
      return "";
    }

  void print()
  {
    {
      map < string, RelInf >::iterator it;
      for (it = rels.begin(); it != rels.end(); it++ )
        {
          cout << (*it).first << " relation has information " << endl << (*it).second << endl;
        }
    }
    {
      map < string, string>::iterator it;
      for (it = extantAttrs.begin(); it != extantAttrs.end(); it++ )
        {
          cout << (*it).first << " is in relation " << (*it).second << endl;
        }
    }
    { // merged relations
      cout << "Merged relations are " << endl;
      map < string, string>::iterator it;
      for (it = mergedRelations.begin(); it != mergedRelations.end(); it++ )
        {
          cout << (*it).first << " is in relation " << (*it).second << endl;

        }
    }
  }
};

#endif