#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include "dberror.h"
#include "storage_mgr.h"

/*int main(){
	
	initStorageManager();
	
	// Test Variables
	char file_Name[10] = TESTFILE;
    char *filePointer = &file_Name[0];
    SM_FileHandle *fHandle = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));
    SM_PageHandle memPage = (SM_PageHandle)malloc(PAGE_SIZE);
    
    // Test calls
	printf("openPageFile: %d\n" ,openPageFile(filePointer, fHandle));
	printFileHandle(fHandle);
	printf("appendEmptyBlock: %d\n", appendEmptyBlock(fHandle));
	printFileHandle(fHandle);
	printf("appendEmptyBlock: %d\n", appendEmptyBlock(fHandle));
	printFileHandle(fHandle);
	printf("readBlock: %d\n" , readBlock(3, fHandle, memPage));
	printFileHandle(fHandle);
	//printf("Ensure capacity: %d\n", ensureCapacity(4,fHandle));
	//printFileHandle(fHandle);
	printf("closePageFile: %d\n" ,closePageFile(fHandle));
	printf("destroyPageFile: %d\n" ,destroyPageFile(fHandle->fileName));
	
	free(fHandle);
	free(memPage);
	
	return 0;
}*/

/****************************************************************
 * Function Name: printFileHandle
 * 
 * Description: Utility to print the contents of the file handle.
 * 
 * Parameter: void
 * 
 * Return: void
 * 
 * Author: Sahil chalke (schalke@hawk.iit.edu)
 ****************************************************************/
void printFileHandle(SM_FileHandle *fHandle){
	
	printf("File name: %s\n", fHandle->fileName);
	printf("Current Page Pos: %d\n", fHandle->curPagePos);
	printf("Total Pages: %d\n\n", fHandle->totalNumPages);

}
/****************************************************************
 * Function Name: initStorageManager
 * 
 * Description: Can be used to initializes the storage manager.
 * 
 * Parameter: void
 * 
 * Return: void
 * 
 * Author: Dhruvit Modi (dmodi2@hawk.iit.edu)
 ****************************************************************/ 	
extern void initStorageManager (void){
  
   //char file_Name[10] = TESTFILE;
   //char *filePointer = &file_Name[0];
   //createPageFile(filePointer);
}

/****************************************************************
 * Function Name: createPageFile
 * 
 * Description: Create a page file and write all '\0' in the 
 *              first page.
 * 
 * Parameter: char *fileName: filename
 * 
 * Return: RC (int)
 * 
 * Author: Dhruvit Modi (dmodi2@hawk.iit.edu)
 * 
 ****************************************************************/	
extern RC createPageFile (char *fileName){
  
  int error;
  if(fileName == NULL){
	  return RC_FILE_NOT_FOUND;
  }
  
  FILE *fp;
  char *initChar = (char *)calloc(1, PAGE_SIZE);
  fp = fopen(fileName, "w+b");
  error = fwrite(initChar, PAGE_SIZE, 1, fp);
  if(error != 1){
	  fclose(fp);
	  return RC_WRITE_FAILED;
  }
  fclose(fp);
  free(initChar);
  return RC_OK; 
}

/****************************************************************
 * Function Name: openPageFile
 * 
 * Description: Create a page file and write all '\0' in the first 
 *              page. Initialize the file handle with file info.
 * 
 * Parameter: char *, SM_FileHandle
 * 
 * Return: RC (int)
 * 
 * Author: Dhruvit Modi (dmodi2@hawk.iit.edu)
 ****************************************************************/
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle){
  
  FILE *fp;
  fp = fopen(fileName, "r+b");
  if (fp == NULL){
    return RC_FILE_NOT_FOUND;
  }
  
  //Initializing the File Handle  
  fHandle->fileName = fileName;
  fHandle->mgmtInfo = (FILE *)fp;
  fHandle->curPagePos = 0;
  fseek(fp, 0L, SEEK_END);
  fHandle->totalNumPages = (int) ftell(fp)/PAGE_SIZE;
  return RC_OK;
}

/****************************************************************
 * Function Name: closePageFile
 * 
 * Description: Closes the page file pointed by the fHandle 
 *              mgmtInfo pointer.
 * 
 * Parameter: char *fileName: filename
 * 
 * Return: RC (int)
 * 
 * Author: Dhruvit Modi (dmodi2@hawk.iit.edu)
 ****************************************************************/
extern RC closePageFile (SM_FileHandle *fHandle){
	
	int error;
	if(fHandle->mgmtInfo == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	}
	error = fclose(fHandle->mgmtInfo);
	if(error){
		return RC_FILE_CLOSE_ERROR;
    }	
	return RC_OK;
}

/****************************************************************
 * Function Name: destroyPageFile
 * 
 * Description: Deletes the page file from the disk.
 * 
 * Parameter: char *
 * 
 * Return: RC (int)
 * 
 * Author: Dhruvit Modi (dmodi2@hawk.iit.edu)
 ****************************************************************/
extern RC destroyPageFile (char *fileName){
	
	int desRet;
	desRet = remove(fileName);
	if(desRet != 0){
		return RC_FILE_DESTROY_ERORR;
	}
	return RC_OK;

}

/****************************************************************
 * Function Name: readBlock
 * 
 * Description: Reads the pageNumth block of the file.
 * 
 * Parameter: int, SM_FileHandle, SM_PageHandle
 * 
 * Return: RC (int)
 * 
 * Author: Dhruvit Modi (dmodi2@hawk.iit.edu)
 ****************************************************************/
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
  
  long error;
  if(fHandle == NULL){
	  return RC_FILE_HANDLE_NOT_INIT;
  }
  
  if(fHandle->totalNumPages <= pageNum){
	  memPage = NULL;
	  return RC_READ_NON_EXISTING_PAGE;
  }
  
  int offset = pageNum*PAGE_SIZE;
  fseek(fHandle->mgmtInfo, offset, SEEK_SET);
  
  error = fread(memPage, PAGE_SIZE, 1, fHandle->mgmtInfo);
  if(error != 1){
	  return RC_READ_ERROR;
  }
  fHandle->curPagePos = pageNum;
  return RC_OK;
}

/****************************************************************
 * Function Name: getBlockPos
 *  
 * Description: Returns the current block position.
 * 
 * Parameter: SM_FileHandle
 * 
 * Return: RC (int)
 * 
 * Author: Monika Priyadarshani (mpriyad1@hawk.iit.edu)
 ****************************************************************/
extern int getBlockPos (SM_FileHandle *fHandle){
	
	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
    }
    return fHandle->curPagePos;
}

/****************************************************************
 * Function Name: appendEmptyBlock
 * 
 * Description: Appends an empty block of all '\0' to the end 
 *              of the file.
 * 
 * Parameter: SM_FileHandle
 * 
 * Return: RC (int)
 * 
 * Author: Monika Priyadarshani (mpriyad1@hawk.iit.edu)
 ****************************************************************/
extern RC appendEmptyBlock (SM_FileHandle *fHandle)
{
	int error;
	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	}
	
	fseek(fHandle->mgmtInfo,0L,SEEK_END);
	char *initChar = (char *)calloc(1, PAGE_SIZE);
    error = fwrite(initChar, PAGE_SIZE, 1, fHandle->mgmtInfo);
    if(error != 1){
	  return RC_WRITE_FAILED;
    }	
	fHandle->totalNumPages=fHandle->totalNumPages + 1;
	free(initChar);
	return RC_OK;
  }
  
 /****************************************************************
 * Function Name: ensureCapacity
 * 
 * Description: Increase the file size to numberOfPages if the file 
 *              has less than numberOfPages pages.
 * 
 * Parameter: int, SM_FileHandle
 * 
 * Return: RC (int)
 * 
 * Author: Monika Priyadarshani (mpriyad1@hawk.iit.edu)
 ****************************************************************/
 extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
 {
     RC error;
	 if(fHandle == NULL)
	 {
		return RC_FILE_HANDLE_NOT_INIT;
	 }
	 if(fHandle->totalNumPages < numberOfPages) 
	 {
		 while((numberOfPages - fHandle->totalNumPages)!=0)
		 {
			error = appendEmptyBlock(fHandle);
			if(!error){
			  continue;
		    }
			return error;
		 }	 
	 }
	 return RC_OK;
}

 /****************************************************************
 * Function Name: writeBlock
 * 
 * Description: Writes contents from memPage(memory) to a pageNumth 
 *              block of the file.
 * 
 * Parameter: int, SM_FileHandle, SM_PageHandle
 * 
 * Return: RC (int)
 * 
 * Author: Sahil Chalke (schalke@hawk.iit.edu)
 ****************************************************************/   
 extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
  
  int error;
  if(fHandle == NULL)
  {
    return RC_FILE_HANDLE_NOT_INIT;		
  }
  if(pageNum > (fHandle -> totalNumPages) || pageNum < 0){
	return RC_WRITE_FAILED;
  }
 
  fseek(fHandle->mgmtInfo, pageNum*PAGE_SIZE, SEEK_SET);
 
	error = fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
	if(error != PAGE_SIZE){
		return RC_WRITE_FAILED;
	}
	fHandle->curPagePos = pageNum + 1;
	return RC_OK;
}

/****************************************************************
 * Function Name: writeCurrentBlock
 * 
 * Description: Writes contents from memPage(memory) to a current 
 *              block of the file.
 * 
 * Parameter: int, SM_FileHandle, SM_PageHandle
 * 
 * Return: RC (int)
 * 
 * Author: Sahil Chalke (schalke@hawk.iit.edu)
 ****************************************************************/
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
  
  return writeBlock(fHandle->curPagePos, fHandle, memPage);	
  
}

/****************************************************************
 * Function Name: readFirstBlock
 * 
 * Description: Reads the first block of the file into the memory
 *              location pointed by memPage.
 * 
 * Parameter: SM_FileHandle, SM_PageHandle
 * 
 * Return: RC (int)
 * 
 * Author: Sahil Chalke (schalke@hawk.iit.edu)
 ****************************************************************/
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
   
   int error;
   if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	}
   fseek(fHandle->mgmtInfo , 0 , SEEK_SET);
   error = fread(memPage, PAGE_SIZE, 1, fHandle->mgmtInfo);
   if(error != 1){
	   return RC_READ_ERROR;
   }
   fHandle->curPagePos = (ftell(fHandle->mgmtInfo)/PAGE_SIZE) - 1;
   return RC_OK;

}

/****************************************************************
 * Function Name: readLastBlock
 * 
 * Description: Reads the last block of the file into the memory
 *              location pointed by memPage.
 * 
 * Parameter: SM_FileHandle, SM_PageHandle
 * 
 * Return: RC (int)
 * 
 * Author: Anirudh Deshpande (adeshp17@hawk.iit.edu)
 ****************************************************************/
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
   
   int error;
   if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	}
	fseek(fHandle->mgmtInfo , -PAGE_SIZE , SEEK_END);
	error = fread(memPage, PAGE_SIZE, 1, fHandle->mgmtInfo);
	if(error != 1){
	   return RC_READ_ERROR;
    }
	fHandle->curPagePos = fHandle->totalNumPages - 1;
	return RC_OK;
}

/****************************************************************
 * Function Name: readPreviousBlock
 * 
 * Description: Reads the previous block of the file into the memory
 *              location pointed by memPage.
 * 
 * Parameter: SM_FileHandle, SM_PageHandle
 * 
 * Return: RC (int)
 * 
 * Author: Anirudh Deshpande (adeshp17@hawk.iit.edu)
 ****************************************************************/
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	int error;
    if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;

	fseek(fHandle->mgmtInfo , -PAGE_SIZE , SEEK_CUR);
	
	error = fread(memPage, PAGE_SIZE, 1, fHandle->mgmtInfo);
	if(error != 1){
	   return RC_READ_ERROR;
    }	
	fHandle->curPagePos = fHandle->curPagePos - 1;
	return RC_OK;
}

/****************************************************************
 * Function Name: readCurrentBlock
 * 
 * Description: Reads the current block of the file into the memory
 *              location pointed by memPage.
 * 
 * Parameter: SM_FileHandle, SM_PageHandle
 * 
 * Return: RC (int)
 * 
 * Author: Anirudh Deshpande (adeshp17@hawk.iit.edu)
 ****************************************************************/
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	int error;
    if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;
		
	
	fseek(fHandle->mgmtInfo , (fHandle->curPagePos)*PAGE_SIZE , SEEK_SET);
	
    error = fread(memPage, PAGE_SIZE, 1, fHandle->mgmtInfo);
    if(error != 1){
	   return RC_READ_ERROR;
    }
	fHandle->curPagePos = fHandle->curPagePos;
    return RC_OK;
}

/****************************************************************
 * Function Name: readNextBlock
 * 
 * Description: Reads the next block of the file into the memory
 *              location pointed by memPage.
 * 
 * Parameter: SM_FileHandle, SM_PageHandle
 * 
 * Return: RC (int)
 * 
 * Author: Anirudh Deshpande (adeshp17@hawk.iit.edu)
 ****************************************************************/
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	int error;
    if (fHandle == NULL)
		return RC_FILE_HANDLE_NOT_INIT;

	
    fseek(fHandle->mgmtInfo , (fHandle->curPagePos + 1) * PAGE_SIZE , SEEK_SET);

    error = fread(memPage, PAGE_SIZE, 1, fHandle->mgmtInfo);
    if(error != 1){
	   return RC_READ_ERROR;
    }	
	fHandle->curPagePos = fHandle->curPagePos + 1;
    return RC_OK;
}
