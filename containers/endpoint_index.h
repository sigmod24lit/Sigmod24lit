#pragma once
#ifndef _ENDPOINT_INDEX_H_
#define _ENDPOINT_INDEX_H_

#include "../def_global.h"
#include "../containers/relation.h"



class EndPointIndexEntry
{
public:
	Timestamp endpoint;
	bool isStart;
	RecordId rid;
	
	EndPointIndexEntry();
	EndPointIndexEntry(Timestamp t, bool isStart, RecordId rid);
	bool operator < (const EndPointIndexEntry& rhs) const;
	bool operator <= (const EndPointIndexEntry& rhs) const;
	void print() const;
	void print(char c) const;
	~EndPointIndexEntry();
};

class EndPointIndex : public vector<EndPointIndexEntry>
{
public:
	size_t numEntries;
	size_t maxOverlappingRecordCount;

	EndPointIndex();
	void build(const Relation &R, size_t from, size_t by);
	void print(char c);
	~EndPointIndex();
};

typedef vector<EndPointIndexEntry>::const_iterator EndPointIndexIterator;
#endif //_ENDPOINT_INDEX_H_
