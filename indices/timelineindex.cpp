 #include "timelineindex.h"



TimelineIndexEntry::TimelineIndexEntry(RecordId id, Timestamp endpoint, bool isStart)
{
	this->id = id;
	this->endpoint = endpoint;
	this->isStart = isStart;
}

TimelineIndex::TimelineIndex(unsigned int checkpointFrequency)
{
	this->checkpointFrequency = checkpointFrequency;
	this->checkpointTimestamp = checkpointFrequency;
}


void TimelineIndex::insert(RecordId id, Timestamp endpoint, bool isStart){

	if (endpoint > checkpointTimestamp || (endpoint == checkpointTimestamp && isStart == false))
		createCheckpoint(endpoint);

	if(isStart)
		deltaCheckPoint.emplace_back(1);
	else
		deltaCheckPoint[id] = 0;		

	eventList.emplace_back(TimelineIndexEntry(id, endpoint, isStart));
}

void TimelineIndex::createCheckpoint(Timestamp endpoint){

		while(endpoint > checkpointTimestamp){
			deltaCheckPoint.checkpointTimestamp = checkpointTimestamp;
			deltaCheckPoint.checkpointSpot = eventList.size();
			VersionMap.emplace_back(deltaCheckPoint);
			checkpointTimestamp+=checkpointFrequency;
		}
		
}

void TimelineIndex::printCheckpoints(){
	int start = 0, end = 0;
	for (auto x : VersionMap){
		end = x.checkpointSpot;
		for(int i=start; i < end; i++){
			cout << this->eventList[i].endpoint << endl;
		}
		cout << " " << endl;
		cout << "checkpointSpot: " << x.checkpointSpot << " last timestamp before checkpoint: " << this->eventList[x.checkpointSpot - 1].endpoint << " first timestamp after checkpoint: " << this->eventList[x.checkpointSpot].endpoint << endl;
		cout << " " << endl;
		start = x.checkpointSpot;
	}
}

inline bool compare(const TimelineIndexEntry &lhs, const TimelineIndexEntry &rhs)
{
        return ((lhs.endpoint < rhs.endpoint) || ((lhs.endpoint == rhs.endpoint) && (lhs.isStart) && (!rhs.isStart)));

}

size_t TimelineIndex::execute_pureTimeTravel(RangeQuery Q)
{
	size_t result = 0;
	int closestValidCheckpointIndex = ((Q.start) / this->checkpointFrequency);
	closestValidCheckpointIndex-=1;



	int qStartPoint = 0;
	vector<TimelineIndexEntry>::iterator qstartBound = this->eventList.begin();
	vector<TimelineIndexEntry>::iterator qendBound = this->eventList.end();
	vector<TimelineIndexEntry>::iterator iterStart = this->eventList.begin();
	vector<TimelineIndexEntry>::iterator iterEnd = this->eventList.end();

	TimelineIndexEntry qDummyS,qDummyE;
	qDummyS.endpoint = Q.start;
	qDummyS.isStart = 1;
	qDummyE.endpoint = Q.end;
	qDummyE.isStart = 1;
	qstartBound = std::lower_bound(iterStart, iterEnd,qDummyS, compare);
	qendBound = std::upper_bound(iterStart, iterEnd,qDummyE, compare);

	if(closestValidCheckpointIndex == -1){
		unordered_set<RecordId> resultsBeforeQstart;
		for(auto iter =  this->eventList.begin(); iter!=qstartBound; iter++){
			if((*iter).isStart)
				resultsBeforeQstart.insert((*iter).id);
			else
				resultsBeforeQstart.erase((*iter).id);
		}
		for(auto x : resultsBeforeQstart)
			result^=x;
	}else{
		CheckPoint temp(this->VersionMap[closestValidCheckpointIndex]);
		iterStart = this->eventList.begin() + this->VersionMap[closestValidCheckpointIndex].checkpointSpot;

		for(auto iter =  iterStart; iter!=qstartBound; iter++){
			if((*iter).isStart)
				temp.emplace_back(1);
			else
				temp[(*iter).id] = 0;
		}


		for(int i = 0; i < temp.size(); i++)
			if(temp[i])
				result^= i;


	}

	
	for(auto iter = qstartBound; iter!= qendBound; iter++)
		if((*iter).isStart)
				result^= (*iter).id;
	


	return result;
	

}



void TimelineIndex::getStats()
{
	cout << VersionMap.size() << " Checkpoints created." << endl;
	for(auto x : VersionMap)
		cout << "checkpointTimestamp: " << x.checkpointTimestamp << " checkpointSpot: " << x.checkpointSpot << " first entry of event list after checkpoint: " << this->eventList[x.checkpointSpot].endpoint << endl; 
}


TimelineIndex::~TimelineIndex()
{

}



