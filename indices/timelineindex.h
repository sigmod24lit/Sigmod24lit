#ifndef _TIMELINEINDEX_H_
#define _TIMELINEINDEX_H_

#include "../def_global.h"
#include "../containers/relation.h"
#include <boost/dynamic_bitset.hpp>
#include "../containers/endpoint_index.h"
#include <unordered_set>
using namespace std; 




class TimelineIndexEntry
{
public:

    RecordId id;
    Timestamp endpoint;
    int secondAttr;
    bool isStart;
    TimelineIndexEntry(){};
    TimelineIndexEntry(RecordId id, Timestamp endpoint, bool isStart);
    TimelineIndexEntry(RecordId id, Timestamp endpoint, bool isStart, int secondAttr);
};

class CheckPoint : public vector<bool>
{
private:

public:
    CheckPoint(){};
    // int* validIntervalIds;

    Timestamp checkpointTimestamp;
    int checkpointSpot;

};



class TimelineIndex
{
private:
    Timestamp checkpointTimestamp;
public:
    RecordId numRecords;
    CheckPoint deltaCheckPoint;
    vector<CheckPoint> VersionMap;
    unsigned int checkpointFrequency;
    int min,max;
    vector<TimelineIndexEntry> eventList;

    // Construction
    TimelineIndex(unsigned int checkpointFrequency);
    void insert(RecordId id, Timestamp endpoint, bool isStart);
    void insert(RecordId id, Timestamp endpoint, bool isStart,  int secondAttr);
    void createCheckpoint(Timestamp endpoint);
    void printCheckpoints();
    void getStats();
    ~TimelineIndex();

    // Querying
    size_t execute(StabbingQuery Q);
    size_t execute_pureTimeTravel(RangeQuery Q);
    size_t executeTimeTravel_greaterthan(RangeQuery Q, int secondAttrConstraint);
    size_t executeTimeTravel_lowerthan(RangeQuery Q, int secondAttrConstraint);
    size_t executeTimeTravel(RangeQuery Q, int lowerConstraint, int upperConstraint);
};
#endif // _PERIODINDEX_H_
