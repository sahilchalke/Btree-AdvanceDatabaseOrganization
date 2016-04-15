// Standard C libraries
#include<stdio.h>
#include<stdlib.h>
#include<pthread.h>
// Local libraries
#include "buffer_mgr.h"


/****************Thread Safe extra credit**********************/

/***** Description: Mutex locks allows only single thread to execute the critical 
 *                  section of the program by blocking all the other processes try to execute concurrently
 *      
 *       Author: Anirudh Deshpande  (adeshp17@hawk.iit.edu)      */
static pthread_mutex_t mutex_init = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_unpinPage = PTHREAD_MUTEX_INITIALIZER; 
static pthread_mutex_t mutex_pinPage = PTHREAD_MUTEX_INITIALIZER;  

int readCount = 0, writeCount = 0, hit = 0;
pageListT *head = NULL;

/***Replacement stratagies implementation****/

/****************************************************************
 * Function Name: FIFO 
 * 
 * Description: Page replacement strategy to replace page frames
 *               using first in first out algorithm.
 * 
 * Parameter: BM_BufferPool, pageListT
 * 
 * Return: RC (int)
 * 
 * Author: Dhruvit Modi  (dmodi2@hawk.iit.edu)
 ****************************************************************/
extern RC FIF0(BM_BufferPool *const bm, pageListT *pageT){
   
    if(bm == NULL){
      return RC_BUFFER_POOL_NOT_INIT;
    }
    pageListT *node = (pageListT *)bm->mgmtData;

    while(node != NULL){
        node = node->next;
    }
    node = (pageListT *)bm->mgmtData;
    SM_FileHandle fHandle;
    while(node != NULL){
      if(node->fifoBit == 1){
          if(node->fixCount == 0){
             /* If the page is modified by the client, then write the page to disk*/
             if(node->dirtyBit == 1)
             {
                openPageFile(bm->pageFile, &fHandle);
                writeBlock(node->pgNum, &fHandle, node->data);
                closePageFile(&fHandle);
                writeCount++;
             }
             node->data = pageT->data;
             node->pgNum = pageT->pgNum;
             node->dirtyBit = 0;
             node->fifoBit = 0;
             if(node->next == NULL){
               head->fifoBit = 1;
             }else{
               node->next->fifoBit = 1;   
             }
             node->fixCount = 1;
             return RC_OK;
        }
        node->next->fifoBit = 1;
      }
      node = node->next;
    }   
    return RC_NO_UNPINNED_PAGES_IN_BUFFER_POOL;
}


/****************************************************************
 * Function Name: LRU 
 * 
 * Description: Page replacement strategy to replace page frames
 *               using least recently used algorithm.
 * 
 * Parameter: BM_BufferPool, pageListT
 * 
 * Return: RC (int)
 * 
 * Author: Anirudh Deshpande  (adeshp17@hawk.iit.edu)
 ****************************************************************/
 extern RC LRU(BM_BufferPool *const bm,  pageListT *pageT)
{ 
    pageListT *node = (pageListT *)bm->mgmtData;
    int min = hit+1;
   
    while(node != NULL)
    {
        if(node->hitrate < min && node->fixCount == 0){
            min = node->hitrate;
        }
        node = node->next;
    }
    node = (pageListT *)bm->mgmtData;
    while(node != NULL){
        if(node->hitrate == min){
                if(node->dirtyBit==1){
                    SM_FileHandle fHandle;
                    openPageFile(bm->pageFile, &fHandle);
                       writeBlock(node->pgNum, &fHandle, node->data);
                       closePageFile(&fHandle);
                       writeCount++;
                       hit++;
                 }    
            node->data = pageT->data;
            node->pgNum = pageT->pgNum;
            node->dirtyBit = pageT->dirtyBit;
            node->fixCount = pageT->fixCount;
            node->hitrate = pageT->hitrate;
            return RC_OK;
        }
        node= node->next;
   
    }
    return RC_NO_UNPINNED_PAGES_IN_BUFFER_POOL;
}
 
/***Pool Handeling implementation****/

/****************************************************************
 * Function Name: initBufferPool 
 * 
 * Description: Creates new buffer pool with numPages 
 *                        
 * 
 * Parameter: BM_BufferPool, pageFileName, numPages, ReplacementStrategy, stratData
 * 
 * Return: RC (int)
 * 
 * Author: Dhruvit Modi  (dmodi2@hawk.iit.edu)
 ****************************************************************/
RC initBufferPool(BM_BufferPool *const bm, char *pageFileName,
                    const int numPages, ReplacementStrategy strategy,
                    void *stratData){
  						
  pthread_mutex_lock(&mutex_init); 				
						
  head = (pageListT *)malloc(sizeof(pageListT));                       
  bm->pageFile = pageFileName;
  bm->numPages = numPages;
  bm->strategy = strategy;
  writeCount = 0;
  readCount = 0;
  hit=0;
 
   // Initilize head of the page frame. 
    head->data = NULL;
    head->fixCount = 0;
    head->dirtyBit = 0;
    head->fifoBit = 1;
    head->pgNum = NO_PAGE;
    head->next = NULL;
    head->hitrate=0;
   
  int bufferSize = numPages;
 
  //pageListT *node = NULL;
  while(bufferSize > 1){
    initPageFrame(head);
    bufferSize--;
  }
  //bm->mgmtData = node;
  bm->mgmtData = head;
  pthread_mutex_unlock(&mutex_init); 
  return RC_OK;
}

/****************************************************************
 * Function Name: initPageFrame 
 * 
 * Description: Creates page frames to be accomodated in the buffer pool.
 * 
 * Parameter: pageListT
 * 
 * Return: RC (int)
 * 
 * Author: Dhruvit Modi  (dmodi2@hawk.iit.edu)
 ****************************************************************/
RC initPageFrame(pageListT* head_ref){
   
    //Represents a page in a frame.
    pageListT* current = head_ref;
    while(current->next != NULL){
        current = current->next;
    }
    current->next = (pageListT *)malloc(sizeof(pageListT));
    current->next->data = NULL;
    current->next->fixCount = 0;
    current->next->dirtyBit = 0;
    current->next->fifoBit = 0;
    current->next->pgNum = NO_PAGE;
    current->next->next = NULL;
    current->next->hitrate=0;
    return RC_OK;
}

/****************************************************************
 * Function Name: shutdownBufferPool 
 * 
 * Description: Destroyes the buffer pool and write all the dirty pages back 
 *              to disk if any found.
 * 
 * Parameter: BM_BufferPool
 * 
 * Return: RC (int)
 * 
 * Author: Sahil Chalke  (schalke@hawk.iit.edu)
 ****************************************************************/
 
 
RC shutdownBufferPool(BM_BufferPool *const bm){
  if(bm == NULL){
      return RC_BUFFER_POOL_NOT_INIT;
  }
 
  pageListT *node = (pageListT *)bm->mgmtData;
 
  /*Check each node to see if its fix count is 0*/ 
  while(node != NULL){
      if(node->fixCount != 0)
        return RC_BUFFER_POOL_CONTAINS_PINNED_PAGES;
      node = node->next; 
  }
  forceFlushPool(bm);
  free(node);
  bm->mgmtData = NULL;
  return RC_OK;
}

/****************************************************************
 * Function Name: forceFlushPool
 * 
 * Description: Writes all dirty pages from Buffer pool to the disk.
 * 
 * Parameter: BM_BufferPool
 * 
 * Return: RC (int)
 * 
 * Author: Sahil Chalke  (schalke@hawk.iit.edu)
 ****************************************************************/
 
 
RC forceFlushPool(BM_BufferPool *const bm){
	
  if(bm == NULL){
      return RC_BUFFER_POOL_NOT_INIT;
  }
 
  pageListT *node = (pageListT *)bm->mgmtData;
  SM_FileHandle fHandle;
 
  /*Check each node to see if its dirty bit is set and fix count is 0*/ 
  while(node != NULL){
      if(node->dirtyBit == 1 && node->fixCount == 0){
        openPageFile(bm->pageFile, &fHandle);
        writeBlock(node->pgNum, &fHandle, node->data);
        closePageFile(&fHandle);
        node->dirtyBit = 0;
        writeCount++;
      }
      node = node->next;
  }
  return RC_OK;
}

/*****Buffer Manager Access implementation****/

/****************************************************************
 * Function Name: markDirty 
 * 
 * Description: Marks a page as a dirty page.
 * 
 * Parameter: BM_BufferPool, BM_PageHandle
 * 
 * Return: RC (int)
 * 
 * Author: Sahil Chalke  (schalke@hawk.iit.edu)
 ****************************************************************/
 
 
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
 
  if(bm == NULL){
    return RC_BUFFER_POOL_NOT_INIT;
  }
  pageListT *node = (pageListT *)bm->mgmtData;
  while(node != NULL){
      if(node->pgNum == page->pageNum){
        node->data = page->data; 
        node->dirtyBit = 1;
        return RC_OK;
      }
      node = node->next;
  }
    return RC_PAGE_NOT_PINNED_IN_BUFFER_POOL;
}


/****************************************************************
 * Function Name: forcePage 
 * 
 * Description: Writes the current page content back to the 
 *              pagefile on disk.
 * 
 * Parameter: BM_BufferPool, BM_PageHandle
 * 
 * Return: RC (int)
 * 
 * Author: Anirudh Deshpande  (adeshp17@hawk.iit.edu) 
 ****************************************************************/


RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){

  if(bm == NULL){
      return RC_BUFFER_POOL_NOT_INIT;
  }
 
  pageListT *node = (pageListT *)bm->mgmtData;
  SM_FileHandle fHandle;
 
  while(node->pgNum != page->pageNum){
      node = node->next;
  }
  if(node == NULL)
    return RC_PAGE_NOT_PINNED_IN_BUFFER_POOL;
  if(node->dirtyBit == 1){ 
    openPageFile(bm->pageFile, &fHandle);
    writeBlock(node->pgNum, &fHandle, node->data);
    node->dirtyBit = 0; 
    writeCount++;
   
  }
 
  return RC_OK;
}

/****************************************************************
 * Function Name: pinPage 
 * 
 * Description: Pins the page with page number pageNum.
 * 
 * Parameter: BM_BufferPool, BM_PageHandle, PageNumber
 * 
 * Return: RC (int)
 * 
 * Author: Dhruvit Modi  (dmodi2@hawk.iit.edu)
 ****************************************************************/

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
        const PageNumber pageNum){
			
    pthread_mutex_lock(&mutex_pinPage);          
    if(bm == NULL){
      return RC_BUFFER_POOL_NOT_INIT;
    }       
         
             
      pageListT *node = (pageListT *)bm->mgmtData;
     
      //SM_FileHandle fHandle;
      RC readError;
      // No pages in memory.
      if(node->pgNum == NO_PAGE){  
        SM_FileHandle fHandle;   
        openPageFile(bm->pageFile, &fHandle);
        node->data = (SM_PageHandle)malloc(PAGE_SIZE);
        readError = readBlock(pageNum, &fHandle, node->data);
        if(readError == RC_READ_ERROR){
          return readError;
        }
        readCount++;
        hit++;
        node->hitrate = hit;
        node->pgNum = pageNum;
        node->fixCount++;
        page->pageNum = pageNum;
        page->data = node->data;
        closePageFile(&fHandle);
        pthread_mutex_unlock(&mutex_pinPage);        
        return RC_OK;
    }else{
        while(node != NULL && node->pgNum != NO_PAGE){
          // Page already exist in memory   
          if(node->pgNum == pageNum){
              node->fixCount++;
              hit++;
              node->hitrate = hit;
              page->pageNum = pageNum;
              page->data = node->data;
              pthread_mutex_unlock(&mutex_pinPage);        
              return RC_OK;
          }
          node = node->next;
        }
        // Page not in memory and buffer has spce left.
        if(node != NULL){
            SM_FileHandle fHandle;
            openPageFile (bm->pageFile, &fHandle);
            node->data = (SM_PageHandle)malloc(PAGE_SIZE);
            readError = readBlock(pageNum, &fHandle, node->data);
            if(readError == RC_READ_ERROR){
              return readError;
            }
            closePageFile(&fHandle);
            readCount++;
            hit++;
            node->hitrate = hit;
            node->pgNum = pageNum;
            node->fixCount++;
            page->pageNum = pageNum;
            page->data = node->data;
            pthread_mutex_unlock(&mutex_pinPage);        
            return RC_OK;
        }
        // Page not in memory and buffer full. Replace page
        else{
            SM_FileHandle fHandle;
            pageListT *newNode = (pageListT *)malloc(sizeof(pageListT));
            openPageFile(bm->pageFile, &fHandle);
            newNode->data = (SM_PageHandle)malloc(PAGE_SIZE);
            readError = readBlock(pageNum, &fHandle, newNode->data);
            newNode->pgNum = pageNum;
            if(readError == RC_READ_ERROR){
              return RC_READ_ERROR;
            }
            closePageFile(&fHandle);
            newNode->fixCount = 1;
            newNode->dirtyBit = 0;
            newNode->hitrate = hit;
            readCount++;
            hit++;
            // Implement replacement startegy 
            if(bm->strategy == RS_FIFO)
              FIF0(bm, newNode);
            else if(bm->strategy == RS_LRU)
              LRU(bm, newNode);
            else; 
            page->pageNum = pageNum;
            page->data = newNode->data; 
            pthread_mutex_unlock(&mutex_pinPage);        
            return RC_OK;
        }
    }   
}

/****************************************************************
 * Function Name: unpinPage 
 * 
 * Description: Unpins the page with page number pageNum.
 * 
 * Parameter: BM_BufferPool, BM_PageHandle
 * 
 * Return: RC (int)
 * 
 * Author: Anirudh Deshpande  (adeshp17@hawk.iit.edu) 
 ****************************************************************/

RC unpinPage(BM_BufferPool * const bm, BM_PageHandle * const page) {
   
    if(bm == NULL){
      return RC_BUFFER_POOL_NOT_INIT;
    }       
      pthread_mutex_lock(&mutex_unpinPage);    
             
      pageListT *node = (pageListT *)bm->mgmtData;
        while(node != NULL && node->pgNum != NO_PAGE){   
          if(node->pgNum == page->pageNum){
              node->fixCount--;
              break;
            }
            node = node->next;
        }    
        //printf("Fix Count: %d\n", node->fixCount);  
        pthread_mutex_unlock(&mutex_unpinPage);           
 return RC_OK;
}


/*****Statistic Interface implementation****/

/****************************************************************
 * Function Name: getNumReadIO 
 * 
 * Description: Returns the number of pages that have been read 
 *              from disk since a buffer pool has been initialized.
 * 
 * Parameter: BM_BufferPool.
 * 
 * Return: readCount (int)
 * 
 * Author: Sahil Chalke (schalke@hawk.iit.edu)
 ****************************************************************/


int getNumReadIO (BM_BufferPool *const bm){

       return readCount;

}


/****************************************************************
 * Function Name: getNumWriteIO 
 * 
 * Description: Returns the number of pages written to the page
 *              file since the buffer pool has been initialized.
 * 
 * Parameter: BM_BufferPool.
 * 
 * Return: writeCount (int)
 * 
 * Author: Monika Priyadarshani (mpriyad1@hawk.iit.edu)
 ****************************************************************/

int getNumWriteIO (BM_BufferPool *const bm){

          return writeCount;
}


/****************************************************************
 * Function Name: getDirtyFlags
 * 
 * Description: Returns an boolean array of size numPages where if
 *              a page is dirty the corresponding value is TRUE if  
 *              not then FALSE.
 * 
 * Parameter: BM_BufferPool.
 * 
 * Return: flags (bool)
 * 
 * Author: Monika Priyadarshani (mpriyad1@hawk.iit.edu)
 ****************************************************************/

bool *getDirtyFlags(BM_BufferPool *const bm) {
   
    bool *flags = (bool*)malloc(sizeof(bool) * bm->numPages);
   
    pageListT *node = (pageListT *)bm->mgmtData;
   
    int i;
    for (i = 0; i < bm->numPages; i++) {
        flags[i] = node->dirtyBit;
        node = node->next;
    }

    return flags;
}


/****************************************************************
 * Function Name: getFixCounts 
 * 
 * Description: Returns an integer array of size numPages where it
 *              shows the fix count of a page either 0 or 1.
 * 
 * Parameter: BM_BufferPool.
 * 
 * Return: fixcount (int)
 * 
 * Author: Monika Priyadarshani (mpriyad1@hawk.iit.edu)
 ****************************************************************/

int *getFixCounts(BM_BufferPool *const bm) {
    int *fixcount = malloc(sizeof(bool) * bm->numPages);
   
        pageListT *node = (pageListT *)bm->mgmtData;
   
    int i;
    for (i = 0; i < bm->numPages; i++) {
        fixcount[i] = node->fixCount;
        node = node->next;
    }

    return fixcount;
}


/****************************************************************
 * Function Name: getFrameContents 
 * 
 * Description: Returns a PageNumber array where each element has
 *              the number of the page stored int page frame.
 * 
 * Parameter: BM_BufferPool.
 * 
 * Return: content (int)
 * 
 * Author: Monika Priyadarshani (mpriyad1@hawk.iit.edu)
 ****************************************************************/

PageNumber *getFrameContents(BM_BufferPool *const bm) {
	
	
    int *content = malloc(sizeof(int) * bm->numPages);
    pageListT *node = (pageListT *)bm->mgmtData;
    int i;
    for (i = 0; i < bm->numPages; i++) {
        if (node->pgNum != NO_PAGE) {
            content[i] = node->pgNum;
        } else {
            content[i] = NO_PAGE;
        }
        node = node->next;
    }
    return content;
}
