#include "endpoint_index.h"



EndPointIndexEntry::EndPointIndexEntry()
{
}


EndPointIndexEntry::EndPointIndexEntry(Timestamp t, bool isStart, RecordId rid)
{
	this->endpoint = t;
	this->isStart = isStart;
	this->rid = rid;
}


EndPointIndexEntry::~EndPointIndexEntry()
{
}


bool EndPointIndexEntry::operator < (const EndPointIndexEntry& rhs) const
{
//    Timestamp diff = this->endpoint - rhs.endpoint;
	
//    return ((this->endpoint < rhs.endpoint) || (this->endpoint == rhs.endpoint) && (this->isStart) && (!rhs.isStart) ||
//            (this->endpoint == rhs.endpoint) && (this->isStart) && (rhs.isStart) && (this->rid < rhs.rid));
        return ((this->endpoint < rhs.endpoint) || (this->endpoint == rhs.endpoint) && (this->isStart) && (!rhs.isStart));
}


bool EndPointIndexEntry::operator <= (const EndPointIndexEntry& rhs) const
{
	return !(rhs < *this);
}


void EndPointIndexEntry::print() const
{
	cout << "<" << this->endpoint << ", " << ((this->isStart)? "START": "END") << ", r" << this->rid << ">" << endl;
}


void EndPointIndexEntry::print(char c) const
{
	cout << "<" << this->endpoint << ", " << ((this->isStart)? "START": "END") << ", " << c << this->rid << ">" << endl;
}




EndPointIndex::EndPointIndex()
{

}


void EndPointIndex::build(const Relation &R, size_t from = 0, size_t by = 1)
{
	// If |R|=3, then if by=2 and from=0, the tupleCount=2, and if from=1, the tupleCount=1
    size_t numRecordsR = R.size();
	size_t tupleCount = (numRecordsR + by - 1 - from) / by;


    // Step 1: create event entries.
	this->numEntries = tupleCount * 2;
	this->reserve(this->numEntries);
    for (size_t i = from; i < numRecordsR; i += by)
	{
        this->emplace_back(R[i].start, true,  i);
        this->emplace_back(R[i].end,   false, i);
    }

    // Step 2: sort index.
	sort(this->begin(), this->end());

    this->maxOverlappingRecordCount = R.size();
}





void EndPointIndex::print(char c)
{
	for (const EndPointIndexEntry& endpoint : (*this))
		endpoint.print(c);
}


EndPointIndex::~EndPointIndex()
{
	//delete[] this->entries;
}
