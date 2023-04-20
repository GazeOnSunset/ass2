
#include "ro.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "db.h"

struct page{
    INT oid;
    INT PageId;
    UINT bufid;
    UINT pagesize;
    UINT pin;
    UINT useage;
    UINT ntuples;
    UINT nattrs;
    Tuple *tuples;
}page;

struct bufPool{
    int   nbufs;         // how many buffers
    char  *strategy;      // LRU, MRU, Cycle
    struct page  *pages;
}bufPool;

void init(){
    // do some initialization here.

    // example to get the Conf pointer
    // Conf* cf = get_conf();

    // example to get the Database pointer
    // Database* db = get_db();
    
    printf("init() is invoked.\n");
}

void release(){
    // optional
    // do some end tasks here.
    // free space to avoid memory leak
  
    printf("release() is invoked.\n");
}



_Table* sel(const UINT idx, const INT cond_val, const char* table_name){
    Conf* cf = get_conf();
    Database* db = get_db();
    BufPool bufpool = NULL;
    bufpool = malloc(sizeof(BufPool));
    int pagenumber = 0;
    int pagesize = cf->page_size;
    int tuplesnum_per_page;
    _Table *innertable;
    FILE* des_table = NULL;
    char* dbname = db->path;
    Table temtable;
    bufpool->nbufs = cf->buf_slots;
    char tablepath[100];
    innertable = malloc(sizeof(_Table));
    
    bufpool = initialize_buffer();
    for(int i = 0; i < db->ntables; i++){
        temtable = db->tables[i];
        if(strcmp(temtable.name, table_name) == 0){
            sprintf(tablepath, "%s/%u",dbname,temtable.oid);
            des_table = fopen(tablepath, "r");
            break;
        }
    }
    innertable->nattrs = temtable.nattrs;
    innertable->ntuples = 0;
    tuplesnum_per_page = (pagesize-sizeof(UINT64))/(temtable.nattrs*sizeof(UINT));
    
    pagenumber = temtable.ntuples/tuplesnum_per_page + 1;
    Page pages[pagenumber];
    for(int i = 0; i<pagenumber;i++){
        pages[i] = (struct page *)malloc(sizeof(Page));;
        pages[i]->PageId = i;
        pages[i]->pagesize = cf->page_size;
        pages[i]->ntuples = tuplesnum_per_page;
        pages[i]->nattrs = temtable.nattrs;
        pages[i]->oid = temtable.oid;
    }
    
    
    for(int i = 0; i<(pagenumber-1);i++){
        Page tempage=NULL;
         
        tempage = request_page(pages[i], bufpool, tuplesnum_per_page, temtable.nattrs, tablepath, temtable.oid, pagenumber, temtable.ntuples);
        
        for(int i = 0; i<tempage->ntuples;i++){
            if(tempage->tuples[i][idx] == cond_val){
                innertable->ntuples++;
                innertable->tuples[(innertable->ntuples)-1]= malloc(sizeof(UINT)*tempage->nattrs);
                for(int j = 0;j<innertable->nattrs;j++){
                    innertable->tuples[(innertable->ntuples)-1][j] = tempage->tuples[i][j];
                    printf("%d ",innertable->tuples[(innertable->ntuples)-1][j]);
                }
                
                
            }
        }
        release_page(tempage, bufpool);
    
    
    
    }
   
    for (int i = 0; i < pagenumber; i++) {
        free(pages[i]);
    }
    freebuffer(bufpool);
    

    // invoke log_read_page() every time a page is read from the hard drive.
    // invoke log_release_page() every time a page is released from the memory.

    // invoke log_open_file() every time a page is read from the hard drive.
    // invoke log_close_file() every time a page is released from the memory.

    // testing
    // the following code constructs a synthetic _Table with 10 tuples and each tuple contains 4 attributes
    // examine log.txt to see the example outputs
    // replace all code with your implementation

  
    
    return innertable;
    // return NULL;
}

_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name){
    
    
    UINT attr_location1 = idx1 * sizeof(UINT);
    UINT attr_location2 =  idx2 * sizeof(UINT);
    Conf* cf = get_conf();
    Database* db = get_db();
    char* dbname = db->path;
    Table temtable1;
    Table temtable2;
    UINT pagenum_t1 = cf->buf_slots-1;
    UINT pagenum_t2 = 1;
    FILE *table1;
    FILE *table2;
    char tablepath1[100];
    char tablepath2[100];
    
    
    for(int i = 0; i < db->ntables; i++){
        temtable1 = db->tables[i];
        if(strcmp(temtable1.name, table1_name) == 0){
            sprintf(tablepath1, "%s/%u",dbname,temtable1.oid);
            table1 = fopen(tablepath1, "r");
            break;
        }
    }
    
    for(int i = 0; i < db->ntables; i++){
        temtable2 = db->tables[i];
        if(strcmp(temtable2.name, table2_name) == 0){
            sprintf(tablepath2, "%s/%u",dbname,temtable2.oid);
            table2 = fopen(tablepath2, "r");
            break;
        }
    }
    
    UINT tuples_per_page1 = (cf->page_size - sizeof(UINT64)) / (sizeof(INT) * temtable1.nattrs);
    UINT tuples_per_page2 = (cf->page_size - sizeof(UINT64)) / (sizeof(INT) * temtable2.nattrs);
    UINT pagenumber1 = (temtable1.ntuples/tuples_per_page1)+1;
    UINT pagenumber2 = (temtable2.ntuples/tuples_per_page1)+1;
   
    BufPool bufpool = NULL;
    bufpool = malloc(sizeof(BufPool));
    _Table *result = malloc(sizeof(_Table));
    result->nattrs = temtable1.nattrs + temtable2.nattrs;
    
    
    bufpool = initialize_buffer();
    Page pages1[pagenumber1];
    for(int i = 0; i<pagenumber1;i++){
        pages1[i] = malloc(sizeof(page));
        pages1[i]->PageId = i;
        pages1[i]->pagesize = cf->page_size;
        pages1[i]->ntuples = tuples_per_page1;
        pages1[i]->nattrs = temtable1.nattrs;
        pages1[i]->oid = temtable1.oid;
    }
    
    Page pages2[pagenumber2];
    for(int i = 0; i<pagenumber2;i++){
        pages2[i] = malloc(sizeof(page));
        pages2[i]->PageId = i;
        pages2[i]->pagesize = cf->page_size;
        pages2[i]->ntuples = tuples_per_page2;
        pages2[i]->nattrs = temtable1.nattrs;
        pages2[i]->oid = temtable2.oid;
    }
    for(int i=0;(i<pagenumber1-1);i++){
        Page tempage1=NULL;
        tempage1 = request_page(pages1[i], bufpool, tuples_per_page1, temtable1.nattrs, tablepath1, temtable1.oid, pagenumber1, temtable1.ntuples);
        for(int j=0;j<(pagenumber2-1);j++){
            Page tempage2=NULL;
            tempage2 = request_page(pages2[j], bufpool, tuples_per_page2, temtable2.nattrs, tablepath2, temtable2.oid, pagenumber2, temtable2.ntuples);
            for(int m=0;m<tempage1->ntuples;m++){
                for(int l=0;l<tempage2->ntuples;l++){
                    if(tempage1->tuples[m][idx1]==tempage2->tuples[l][idx2]){
                        int x=0;
                        for(int z=0;z<tempage1->nattrs;z++){
                            result->tuples[result->ntuples][z]=tempage1->tuples[m][z];
                            x++;
                        }
                        for(int y = 0;y<tempage2->nattrs;y++){
                            result->tuples[result->ntuples][x]=tempage2->tuples[l][y];
                            x++;
                        }
                        result->ntuples++;
                        result = realloc(result, sizeof(_Table) + result->ntuples * sizeof(Tuple));
                    }
                }
            }
            release_page(tempage2, bufpool);
        }
        
        release_page(tempage1, bufpool);
    }
    
    
    
    
    
    
    
    
    
    
    // write your code to join two tables
    // invoke log_read_page() every time a page is read from the hard drive.
    // invoke log_release_page() every time a page is released from the memory.

    // invoke log_open_file() every time a page is read from the hard drive.
    // invoke log_close_file() every time a page is released from the memory.

    return NULL;
}






// Initialization
BufPool initialize_buffer() {
    BufPool pool;
    Conf *con = get_conf();
    int slot = con->buf_slots;
    
    pool = malloc(sizeof(struct bufPool));
    pool->nbufs = con->buf_slots;
    pool->strategy = con->buf_policy;
    pool->pages =malloc(slot*sizeof(INT));
    for(int i =0 ;i<slot;i++){
        pool->pages[i].useage = 0;
        pool->pages[i].pin = 0;
        pool->pages[i].PageId = -1;
        pool->pages[i].bufid = i;
        pool->pages[i].ntuples = 0;
        pool->pages[i].nattrs = 0;
        pool->pages[i].oid = 0;
        pool->pages[i].pagesize = con->page_size;
    }

    return pool;
}


// Request a page from the buffer
Page request_page(Page tpage, BufPool buffer,int ntuples, int natts, char *tablepath,int oid, int pagenumber, int alltuples) {
    int nvb = 0;
    // if page in buffers //
    for (int i = 0; i < buffer->nbufs; i++) {
        if (buffer->pages[i].PageId == tpage->PageId && tpage->oid==oid) {
            buffer->pages[i].useage++;
            buffer->pages[i].pin = 1;
            return tpage;
        }
    }
    //if page not in buffers//
    while (true) {
        if (buffer->pages[nvb].pin == 0 && buffer->pages[nvb].useage == 0) {
            buffer->pages[nvb].PageId = tpage->PageId;
            buffer->pages[nvb].nattrs = tpage->nattrs;
            buffer->pages[nvb].ntuples= tpage->ntuples;
            buffer->pages[nvb].tuples = malloc(ntuples*sizeof(UINT));
            for(int i = 0;i<ntuples;i++){
                buffer->pages[nvb].tuples[i] = malloc(natts*sizeof(INT));
            }
            if(buffer->pages[nvb].PageId < pagenumber-1){
                read_page_from_disk(&buffer->pages[nvb],ntuples,natts,tablepath,oid);
                
            }
            else if (buffer->pages[nvb].PageId == pagenumber-1){
                read_lastpage_from_disk(&buffer->pages[nvb],ntuples,natts,tablepath,oid,alltuples);
            }
            buffer->pages[nvb].pin= 1;
            buffer->pages[nvb].useage = 1;
            int curnvb = nvb;
            nvb = (nvb + 1) % buffer->nbufs;
            return &buffer->pages[curnvb];
        } else {
            if (buffer->pages[nvb].useage > 0) buffer->pages[nvb].useage--;
            nvb = (nvb + 1) % buffer->nbufs;
        }
    }
}

// Notify the buffer that the page is no longer in use
void release_page(Page page , BufPool buffer) {
    for (int i = 0; i < buffer->nbufs; i++) {
       
        if (buffer->pages[i].PageId== page->PageId) {
            buffer->pages[i].pin = 0;
            break;
        }
    }
    log_release_page(page->PageId);
}

// Function to read a page from disk
void read_page_from_disk(Page page, int ntuples, int natts, char *tablepath,int oid) {
    int pid = page->PageId;
    int psize = page->pagesize;
    
    FILE *table = fopen(tablepath, "r");
    log_open_file(oid);
    fseek(table, psize*pid+sizeof(UINT64), SEEK_SET);
    for(int i=0;i<page->ntuples;i++){
        for(int j=0;j<natts;j++){
            fread(&page->tuples[i][j], sizeof(INT), 1, table);
        }
    }
    log_read_page(page->PageId);
    fclose(table);
    log_close_file(oid);
}

void read_lastpage_from_disk(Page page, int ntuples, int natts, char *tablepath,int oid,int alltuples) {
    int pid = page->PageId;
    int psize = page->pagesize;
    
    int tuplenum = alltuples%ntuples;
    page->ntuples = tuplenum;
    if(tuplenum == 0){
        FILE *table = fopen(tablepath, "r");
        log_open_file(oid);
        fseek(table, psize*pid+sizeof(UINT64), SEEK_SET);
        for(int i=0;i<page->ntuples;i++){
            for(int j=0;j<natts;j++){
                fread(&page->tuples[i][j], sizeof(INT), 1, table);
            }
        }
        log_read_page(page->PageId);
        fclose(table);
        log_close_file(oid);
        
    }
    else if(tuplenum!=0){
        FILE *table = fopen(tablepath, "r");
        log_open_file(oid);
        fseek(table, psize*pid+sizeof(UINT64), SEEK_SET);
        for(int i=0;i<tuplenum;i++){
            for(int j=0;j<natts;j++){
                fread(&page->tuples[i][j], sizeof(INT), 1, table);
              
               
               
            }
            
        }
        log_read_page(page->PageId);
        fclose(table);
        log_close_file(oid);
    }
}
    


void freebuffer(BufPool buf){
    for(int i = 0; i < buf->nbufs; i++) {
        for(int j = 0; j < buf->pages[i].ntuples; j++) {
            free(buf->pages[i].tuples[j]);
        }
    }
    free(buf->pages);
    free(buf);
}


