#ifndef _OFFSETS_TEMPLATES_H_
#define _OFFSETS_TEMPLATES_H_

#include "relation.h"



template <class T>
class OffsetEntry_SS
{
public:
    Timestamp tstamp;
    typename T::iterator iter;
    PartitionId pid;
    
    OffsetEntry_SS();
    OffsetEntry_SS(Timestamp tstamp, typename T::iterator iter, PartitionId pid);
    bool operator < (const OffsetEntry_SS<T> &rhs) const;
    bool operator >= (const OffsetEntry_SS<T> &rhs) const;
    ~OffsetEntry_SS();
};

// For HINT
typedef OffsetEntry_SS<RelationId>    OffsetEntry_SS_HINT;

// For HINT^m
typedef OffsetEntry_SS<Relation>      OffsetEntry_SS_OrgsIn;
typedef OffsetEntry_SS<RelationStart> OffsetEntry_SS_OrgsAft;
typedef OffsetEntry_SS<RelationEnd>   OffsetEntry_SS_RepsIn;
typedef OffsetEntry_SS<RelationId>    OffsetEntry_SS_RepsAft;



template <class T>
class Offsets_SS : public vector<OffsetEntry_SS<T> >
{
public:
    Offsets_SS();
    ~Offsets_SS();
};

// For HINT
typedef Offsets_SS<RelationId> Offsets_SS_HINT;
typedef Offsets_SS_HINT::const_iterator Offsets_SS_HINT_Iterator;

// For HINT^m
typedef Offsets_SS<Relation>      Offsets_SS_OrgsIn;
typedef Offsets_SS<RelationStart> Offsets_SS_OrgsAft;
typedef Offsets_SS<RelationEnd>   Offsets_SS_RepsIn;
typedef Offsets_SS<RelationId>    Offsets_SS_RepsAft;
typedef Offsets_SS_OrgsIn::const_iterator  Offsets_SS_OrgsIn_Iterator;
typedef Offsets_SS_OrgsAft::const_iterator Offsets_SS_OrgsAft_Iterator;
typedef Offsets_SS_RepsIn::const_iterator  Offsets_SS_RepsIn_Iterator;
typedef Offsets_SS_RepsAft::const_iterator Offsets_SS_RepsAft_Iterator;
#endif //_OFFSETS_TEMPLATES_H_
