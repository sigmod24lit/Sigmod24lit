#include "getopt.h"
#include "def_global.h"
#include "./containers/relation.h"
#include "./indices/timelineindex.h"


void usage()
{
    cerr << endl;
    cerr << "PROJECT" << endl;
    cerr << "       LIT: Lightning-fast In-memory Temporal Indexing" << endl << endl;
    cerr << "USAGE" << endl;
    cerr << "       ./query_timelineindex.exec [OPTIONS] [STREAMFILE]" << endl << endl;
    cerr << "DESCRIPTION" << endl;
    cerr << "       -? or -h" << endl;
    cerr << "              display this help message and exit" << endl;
    cerr << "       -c" << endl;
    cerr << "              set the checkpoint frequency" << endl;      
    cerr << "       -r runs" << endl;
    cerr << "              set the number of runs per query; by default 1" << endl << endl;
    cerr << "EXAMPLE" << endl;
    cerr << "       ./query_timelineindex.exec -c 86400 streams/BOOKS.mix" << endl << endl;
}


int main(int argc, char **argv)
{
    Timer tim;
    Record r;
    TimelineIndex *idxR;;

    size_t totalResult = 0, queryresult = 0, numQueries = 0, numUpdates = 0, numInserts = 0;
    double starttime = 0, endtime = 0, querytime = 0, avgQueryTime = 0;
    double totalIndexTime = 0, totalIndexEndTime = 0, totalQueryTime = 0, totalUpdateTime = 0;
    Timestamp first, second, startEndpoint;
    double third, fourth;
    unsigned int checkpointFrequency = 0;
    RunSettings settings;
    char c, operation;
    double vm = 0, rss = 0, vmMax = 0, rssMax = 0;
    string strQuery = "", strPredicate = "", strOptimizations = "";
    Timestamp leafPartitionExtent = 0;

    
    settings.init();
    settings.method = "timeline-index";
    while ((c = getopt(argc, argv, "?hq:c:r:")) != -1)
    {
        switch (c)
        {
            case '?':
            case 'h':
                usage();
                return 0;

           case 'c':
               checkpointFrequency = atoi(optarg);
               break;
                
            case 'r':
                settings.numRuns = atoi(optarg);
                break;

            default:
                cerr << endl << "Error - unknown option '" << c << "'" << endl << endl;
                usage();
                return 1;
        }
    }
    
    
    if (argc-optind != 1)
    {
        usage();
        return 1;
    }


    
    
    tim.start();
    idxR = new TimelineIndex(checkpointFrequency);
    totalIndexTime = tim.stop();
    

    settings.queryFile = argv[optind];
    ifstream fQ(settings.queryFile);
    if (!fQ)
    {
        usage();
        return 1;
    }

    size_t sumQ = 0;
    size_t count = 0;

    while (fQ >> operation >> first >> second >> third >> fourth)
    {
        switch (operation)
        {
            case 'S':
                numInserts++;
                tim.start();
                idxR->insert(first, second, 1);
                starttime = tim.stop();
                totalUpdateTime+=starttime;
                break;

            case 'E':
                numUpdates++;

                tim.start();
                idxR->insert(first, second, 0);
                endtime = tim.stop();
                totalUpdateTime+=endtime;
                break;

            case 'Q':
                numQueries++;
                sumQ += second-first;

                double sumT = 0;
                for (auto r = 0; r < settings.numRuns; r++)
                {
                    tim.start();
                    queryresult = idxR->execute_pureTimeTravel(RangeQuery(numQueries, first, second));
                    querytime = tim.stop();
                    totalQueryTime += querytime;
                }
                totalResult += queryresult;
                avgQueryTime += sumT/settings.numRuns;
                break;
        }
    }
    fQ.close();
    cout << endl;
    cout << "Timeline index" << endl;
    cout << "==============" << endl;
    cout << "Num of intervals          : " << numUpdates << endl;
    cout << "Num of checkpoints        : " << idxR->VersionMap.size() << endl;
    cout << endl;
    printf("Updating time             : %f secs\n", totalUpdateTime);
    cout << "Query type                : " << strQuery << endl;
    cout << "Num of runs per query     : " << settings.numRuns << endl;
    cout << "Num of queries            : " << numQueries << endl;
    cout << "Total result [XOR]        : " << totalResult << endl;
    printf( "Total querying time [secs]: %f\n", totalQueryTime/settings.numRuns);

    delete idxR;
    
    
    return 0;
}
