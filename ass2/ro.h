//  Created by Zhengdong Chen on 14/4/2023.
//
#ifndef RO_H
#define RO_H
#include "db.h"

void init(void);
void release(void);
typedef struct bufPool *BufPool;
typedef struct page *Page;
// equality test for one attribute
// idx: index of the attribute for comparison, 0 <= idx < nattrs
// cond_val: the compared value
// table_name: table name
_Table* sel(const UINT idx, const INT cond_val, const char* table_name);

_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name);
void read_page_from_disk(Page page, int ntuples, int natts, char *tablepath,int oid);
void read_lastpage_from_disk(Page page, int ntuples, int natts, char *tablepath,int oid,int alltuples);
Page request_page(Page tempage, BufPool buffer,int ntuples, int natts, char *tablepath,int oid, int pagenumber, int alltuples);
void release_page(Page page , BufPool buffer);
BufPool initialize_buffer(void);
void freebuffer(BufPool buf);
