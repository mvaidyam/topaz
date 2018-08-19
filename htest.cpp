// htest.cpp : Defines the entry point for the console application.
//

#include <fstream>
#include <string>
#include <sstream>
#include <iostream>
#include <map>
#include <queue>
#include <unordered_map>
#include <thread>
#include <fstream>
#include "thrdsafequeue.h"

typedef struct input_record
{
    //string symbol;
    __int64 ts;
    int quantity;
    int price;
} INREC;

typedef struct output
{
    std::string symbol;
    __int64 maxTimeGap;
    __int64 ts;
    int     totalvolume;
    int     maxprice;
    int     weightavgprice;
    int     weightedAvgRes;

    output():maxTimeGap(0),ts(0),totalvolume(0),maxprice(0), weightavgprice(0), weightedAvgRes(0) 
    {
    
    };
}OUTREC;

std::string inputfile;
std::string outputfile;
std::map< std::string, std::queue<INREC>> qmap;
std::map< std::string, OUTREC> output;
threadsafe_queue<std::string> msg_queue;



void create_outputrec(const std::string& key)
{
    auto it= output.find(key);
    if( it == output.end())
    {
        std::pair<std::string,OUTREC> p(key,OUTREC());
        output.insert(p);
    }
}

void update_output_diff( const std::string& key, const __int64& cur_ts )
{
    create_outputrec(key);

    OUTREC& r = output[key];
    r.symbol = key;
    if(!r.ts)
        r.ts = cur_ts;
    else
    {
        if( r.maxTimeGap < (cur_ts - r.ts))
            r.maxTimeGap = cur_ts - r.ts;
        r.ts = cur_ts;
        //output[key]= r;
    }
}

void update_output_totalvolume(const std::string& key, const int& quantity, const int& price)
{
    OUTREC& r = output[key];
    r.totalvolume += quantity; 
    if( r.maxprice < price )
        r.maxprice = price;
    r.weightavgprice += quantity * price;
    r.weightedAvgRes = r.weightavgprice/r.totalvolume;
}


void extract(std::string& line)
{
    INREC rec;
    std::string key;

    size_t f1 = line.find_first_of(",");
    
    std::string fld = line.substr(0,f1);
    std::stringstream fld0(fld);
    fld0 >> rec.ts ;
            
    size_t f2 = line.find_first_of(",",f1+1);
    key = line.substr(f1+1, f2-f1-1);
            
    size_t f3 = line.find_first_of(",",f2+1);
    std::stringstream fld1(line.substr(f2+1, f3));
    fld1 >> rec.quantity;

    std::stringstream fld2(line.substr(f3+1));
    fld2 >> rec.price;


    auto it = qmap.find(key);
    if(it == qmap.end())
    {
        std::queue<INREC> v;
        std::pair<std::string,std::queue<INREC>> p(key,v);
        qmap.insert(p);
        qmap[key].push(rec); //place formatted rec in input queue 
    }
    else
        qmap[key].push(rec);
    // hint consumer queue with a message to pick processing.
    msg_queue.push_back(key);
}

void showResults(void)
{
    int sz = output.size();
    int count=1;
    std::ofstream resfile;
    resfile.open(outputfile.c_str());
    std::cout << "Writing to output file at location " << outputfile.c_str() << std::endl;
    auto ite = output.end();
    for(auto it= output.begin(); it !=ite; ++it,count++ )
    {
       // std::cout << (*it).first ;
        OUTREC r = (*it).second;
        std::ostringstream buffer; 
        buffer << r.symbol << "," << r.maxTimeGap << "," << r.totalvolume << "," << r.weightedAvgRes << "," << r.maxprice <<std::endl;
        resfile << buffer.str();
    }
    resfile.close();
}

void producer(void)
{
    std::ifstream file(inputfile.c_str());
    if (file.is_open()) {
        std::string line;
        while (std::getline(file, line)) {
            extract(line);
        }
        file.close();
        msg_queue.push_back("<END>");
    }
}

void consumer(void)
{
    while (1){
        std::shared_ptr<std::string> pKey = msg_queue.try_pop(); 
        if(pKey.get() == nullptr)
            break;
        std::string key =*pKey;

        if(key=="<END>") // simple thought to close the work :)
            break;
// we can plan thread pool here to introduce parallel. 
//          std::thread t([=](void){
                std::queue<INREC>& q = qmap[key];
                while(!q.empty()) {
                    INREC rec = q.front(); q.pop();
                    update_output_diff(key, rec.ts);
                    update_output_totalvolume(key,rec.quantity, rec.price);
                }
//                            });
    }
    showResults();
}

int main(int argc, char* argv[])
{
    if(argc <= 1)
    {
       std::cout << "Usage: exec input.csv output.csv \n";
       exit(1);
    }
    if( argc ==2 )
    {
        std::cout << "Output file name required\n";
        std::cout << "Usage: exec input.csv output.csv \n";
        exit(2);
    }
    if( argc >3)
    {
        std::cout << "Error: too many arguments\n";
        std::cout << "Usage: exec input.csv output.csv \n";
        exit(3);
    }

    std::string in(argv[1]);
    std::string out(argv[2]);

    std::ifstream fi(in.c_str());
    if(!fi.good())
    {
        std::cout << "ERROR: input file does not exist. Path obtained: " << in.c_str() << std::endl; 
        exit(4);
    }
    std::ifstream fo(out.c_str());
    if(fo.good())
    {
        std::cout << "output file name selected is used by another file\n";
        exit(5);
    }
    
    inputfile = in;
    outputfile = out;

    std::vector<std::thread> v;
    v.emplace_back(producer);
    v.emplace_back(consumer);

    for (auto& t : v)
        t.join();
    return 0;
}
