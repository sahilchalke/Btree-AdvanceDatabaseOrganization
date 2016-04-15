#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<math.h>
#include "record_mgr.h"

/**
 * The table metadata has important attributes. It also has a the
 * buffer pool for the table.
 **/
typedef struct Table_Metadata {
  int numTuples;
  int numPages;
  int freePage;
  BM_BufferPool bm;
}Table_Metadata;
Table_Metadata *metaD;

/**
 * This structure holds the data required for scan.
 **/
typedef struct Scan_Metadata {
  RID id;
  Expr *cond;
  int scannedRecords;	
}Scan_Metadata;
//int insertCount;

// Utilitiy to print schema.
void printSchema(RM_TableData *rel);

/***Table and Manager methods****/

/****************************************************************
 * Function Name: initRecordManager
 * 
 * Description: Wrapper method which calls initStorageMethod.
 * 
 * Parameter: void *
 *   
 * Return: Error code (RC)
 * 
 * Author: Dhruvit Modi (dmodi2@hawk.iit.edu)
 ****************************************************************/
extern RC initRecordManager (void *mgmtData){
  // Call to initialize Storage manager.	
  initStorageManager();
  return RC_OK;	
}

/****************************************************************
 * Function Name: shutdownRecordManager
 * 
 * Description: Frees the memory allocated to table metaData 
 *              structure.
 * 
 * Parameter: --
 * 
 * Return: Error code (RC)
 * 
 * Author: Dhruvit Modi (dmodi2@hawk.iit.edu)
 ****************************************************************/
extern RC shutdownRecordManager (){
  // Free memory allocated to table metadata.	
  free(metaD);	
  return RC_OK;	
}

/****************************************************************
 * Function Name: createTable
 * 
 * Description: Created a table with a given schema. Saves the 
 *              to the disk.
 * 
 * Parameter: char *, Schema
 * 
 * Return: Error code (RC)
 * 
 * Author: Dhruvit Modi (dmodi2@hawk.iit.edu)
 ****************************************************************/
extern RC createTable (char *name, Schema *schema){
  
  SM_FileHandle fhandle;
  char fileData[PAGE_SIZE];
  char *info = fileData;
  int i;
  
  //Initialize metadata for the table
  *(int *)info = 0; // Number of tuples = 0.
  info+= sizeof(int); 
  *(int *)info = 1; // Total number of pages = 1.
  info+= sizeof(int);
  *(int *)info = 1; // First free page = 1.
  info+= sizeof(int);
  
  //Writing schema data to page 
  *(int *)info = schema->numAttr;
  info+= sizeof(int);
  *(int *)info = schema->keySize;
  info+= sizeof(int);
  // Iterate through all the attributes of the schema.
  for(i = 0; i < schema->numAttr; i++){
	  
    strncpy(info, schema->attrNames[i], 10);	
    info += 10;

    *(int*)info = (int)schema->dataTypes[i];
    info += sizeof(int);

    *(int*)info = (int) schema->typeLength[i];
    info += sizeof(int); 
  }
  
  //Creating a table and the metadata and schema on the disk.
  if(createPageFile(name) != RC_OK)
    return RC_CREATE_TABLE_FAILED;		
		
  if(openPageFile(name, &fhandle) != RC_OK)	
	return RC_CREATE_TABLE_FAILED;
		
  if(writeBlock(0, &fhandle, fileData) != RC_OK)	
	return RC_CREATE_TABLE_FAILED;
		
  if(closePageFile(&fhandle) != RC_OK)		
    return RC_CREATE_TABLE_FAILED;
  
  return RC_OK;  
}

/****************************************************************
 * Function Name: open table
 * 
 * Description: Retrives table metadata and schema from the disk. Also 
 *              initializes buffer pool for further operations.
 * 
 * Parameter: RM_TableData, char
 * 
 * Return: Error code (RC)
 * 
 * Author: Dhruvit Modi (dmodi2@hawk.iit.edu)
 ****************************************************************/
extern RC openTable (RM_TableData *rel, char *name){
  
  BM_PageHandle *pHandle = MAKE_PAGE_HANDLE();
  metaD = (Table_Metadata *)malloc(sizeof(Table_Metadata));
  char *info = (char *)malloc(PAGE_SIZE);
  rel->mgmtData = (Table_Metadata *)metaD;
  RC rc;
  int i;
  
  //Initializing buffer pool for the given table.
  initBufferPool(&metaD->bm, name, 10, RS_FIFO, NULL);
  rc = pinPage(&metaD->bm, pHandle, 0);
  if(rc != RC_OK)	
	 return RC_CREATE_TABLE_FAILED;
	 
  // Extracting metadata
  info = pHandle->data;
  metaD->numTuples = *(int *)info;
  info += sizeof(int); 
  metaD->numPages = *(int *)info;
  info += sizeof(int);
  metaD->freePage = *(int *)info;
  info += sizeof(int);
  
  // Extracting Schema
  Schema *sc = (Schema *)malloc(sizeof(Schema));
  sc->numAttr = *(int *)info;
  info += sizeof(int);
  sc->keySize = *(int *)info;
  info += sizeof(int);
  
  sc->attrNames = (char **)malloc(sizeof(char*)*sc->numAttr);
  sc->dataTypes = (DataType *)malloc(sizeof(DataType)*sc->numAttr);
  sc->typeLength = (int *)malloc(sizeof(int)*sc->numAttr);
  
  // Iterate through the schema to get all the attributes.
  for(i = 0; i < sc->numAttr; i++){
	
	sc->attrNames[i] = (char *)malloc(10);
	strncpy(sc->attrNames[i], info, 10);	
    info += 10;

    sc->dataTypes[i] = *(int *)info;
    info += sizeof(int);

    sc->typeLength[i] = *(int*)info;
    info += sizeof(int); 
  }
  
  rel->name = name;
  rel->schema = sc;
  unpinPage(&metaD->bm, pHandle);
  
  //Free allocated space
  free(pHandle);
  return RC_OK;
}

void printSchema(RM_TableData *rel){
  
  printf("==================\n");
  Schema *sc = rel->schema;
  int i;
  printf("Number of attributes in schema: %d\n", sc->numAttr);
  printf("Key size: %d\n", sc->keySize);
  
  for(i = 0; i < sc->numAttr; i++){
	  printf("Attr name: %s\n", sc->attrNames[i]);
	  printf("Datatype: %d\n", sc->dataTypes[i]);
	  printf("Typelength: %d\n", sc->typeLength[i]);
  }
  printf("==================\n");
  return;	
}

/****************************************************************
 * Function Name: closeTable
 * 
 * Description: Saves the updated table parameters to the file.
 *              Shutdown the buffer pool and set table metaData to 
 *              NULL.
 * 
 * Parameter: RM_TableData
 * 
 * Return: Error code (RC)
 * 
 * Author: Dhruvit Modi (dmodi2@hawk.iit.edu)
 ****************************************************************/
extern RC closeTable (RM_TableData *rel){

  if(rel == NULL)
     return RC_TABLE_NOT_FOUND;
    
  Table_Metadata *metaD;
  BM_PageHandle *pHandle = MAKE_PAGE_HANDLE();
  char *info = (char *)malloc(PAGE_SIZE);
  RC rc;
 
  metaD = rel->mgmtData;
  pinPage(&metaD->bm, pHandle, 0);
  info = pHandle->data;
  *(int *)info = metaD->numTuples;
  info += sizeof(int); 
  *(int *)info = metaD->numPages;
  info += sizeof(int);
  *(int *)info = metaD->freePage;
  
  // Let buffer manager know that contents of the page have been changed.
  markDirty(&metaD->bm, pHandle);
	
  // Our work is done so unpin the page.
  unpinPage(&metaD->bm, pHandle);
 
  rc = shutdownBufferPool(&metaD->bm);
  if(rc != RC_OK)	
	  return rc;
  rel->mgmtData = NULL;
  return RC_OK;
 
}

/****************************************************************
 * Function Name: deleteTable
 * 
 * Description: Deletes the table (page file) from the disk.
 * 
 * Parameter: char *
 * 
 * Return: Error code (RC)
 * 
 * Author: Dhruvit Modi (dmodi2@hawk.iit.edu)
 ****************************************************************/
extern RC deleteTable (char *name){

 if(name == NULL)
    return RC_TABLE_NOT_FOUND;
 RC rc;
 // delete the page file from the disk.
 rc = destroyPageFile(name);	
 if(rc != RC_OK)	
	 return rc;
 return RC_OK;
 	 
}

/****************************************************************
 * Function Name: getNumTuples
 * 
 * Description: Retrives the number of tuples in the table.
 * 
 * Parameter: RM_TableData
 * 
 * Return: num of tuples (int)
 * 
 * Author: Dhruvit Modi (dmodi2@hawk.iit.edu)
 ****************************************************************/
extern int getNumTuples (RM_TableData *rel){
  
  if(rel == NULL)
    return RC_TABLE_NOT_FOUND;	
  Table_Metadata *metaD;
  metaD = rel->mgmtData;
  int numTup =  metaD->numTuples; 
  return numTup;
    
}

/***** Methods dealing with records and tables *****/

/****************************************************************
 * Function Name: insertRecord
 * 
 * Description: Insert a given record in the table.
 * 
 * Parameter: RM_TableData, Record
 * 
 * Return: Error code (RC)
 * 
 * Author: Dhruvit Modi (dmodi2@hawk.iit.edu)
 ****************************************************************/
extern RC insertRecord (RM_TableData *rel, Record *record){
	
	Table_Metadata *metaD = rel->mgmtData;
	BM_PageHandle *pHandle = MAKE_PAGE_HANDLE();
	char *info = (char *)malloc(PAGE_SIZE);

	// Setting the page of the record to the first free page.
	record->id.page = metaD->freePage;
	
	// Pin the first available free page.
	pinPage(&metaD->bm, pHandle, record->id.page);
	int recordSize = getRecordSize(rel->schema);
	info = pHandle->data;
	
	// Find first available empty slot in the free page.
	int offset = -1, flag = 0;
	int i, totalNumOfSlots = PAGE_SIZE/recordSize;
	while(offset == -1){
	  // Iterate through all the slots till empty slot (tombstone = '*') is found
      for(i = 0; i < totalNumOfSlots; i++){
	     if(info[recordSize * i] != '*'){
			 offset = i;
			 flag = 1;
			 break;
		 }
      }	
      // Break while loop if empty slot found found.
      if(flag == 1)
        break;
      
      //No empty slots in page.  
      unpinPage(&metaD->bm, pHandle);
	  record->id.page++;
	  if((metaD->numPages - 1) < record->id.page)
	     metaD->numPages = record->id.page;
	  pinPage(&metaD->bm, pHandle, record->id.page);	
	  info = pHandle->data;
	}
	record->id.slot = offset;
	info+= offset*recordSize;
	// used for tombstone purpose. '*' indicates active record. 
	*info = '*';
	info+= 1;
	memcpy(info, record->data, recordSize - 1);
	
	// Let buffer manager know that contents of the page have been changed.
	markDirty(&metaD->bm, pHandle);
	
	// Our work is done so unpin the page.
	unpinPage(&metaD->bm, pHandle);
	metaD->numTuples++;
	
	// Free allocated memory
	free(pHandle);
	return RC_OK;
}

/****************************************************************
 * Function Name: deleteRecord
 * 
 * Description: Deletes a given record from the table. Marks a tombstone
 *              on that record. This inidicates to the record manager that 
 *              this space can be re-used to insert another record.
 * 
 * Parameter: RM_TableData, RID
 * 
 * Return: Error code (rc)
 * 
 * Author: Dhruvit Modi (dmodi2@hawk.iit.edu)
 ****************************************************************/
extern RC deleteRecord (RM_TableData *rel, RID id){
  
    Table_Metadata *metaD = rel->mgmtData;
	BM_PageHandle *pHandle = MAKE_PAGE_HANDLE();
	char *info = (char *)malloc(PAGE_SIZE);
	
	// Extract the page from which we have to delete the record.
	pinPage(&metaD->bm, pHandle, id.page);
	info = pHandle->data;
	
	//Fetch record size from the schema
	int recordSize = getRecordSize(rel->schema);
	
	// Find the offset of the record to be deleted.
	int offset = recordSize * id.slot;
	info+= offset;
	
	// Mark a tombstone on the record to indicate that it is a free slot 
	// and can be used by insertRecord method to insert new record.
	*info = '+';
	
	// Mark this page as a free page.
	metaD->freePage = id.page;
	metaD->numTuples--;
	
	// Let buffer manager know that contents of the page have been changed.
	markDirty(&metaD->bm, pHandle);
	
	// Our work is done so unpin the page.
	unpinPage(&metaD->bm, pHandle);
	
	// Free allocated memory
	free(pHandle);
	return RC_OK;
	
}

/****************************************************************
 * Function Name: updateRecord
 * 
 * Description: Updates a given record in the table.
 * 
 * Parameter: RM_TableData, Record
 * 
 * Return: Error code (RC)
 * 
 * Author: Anirudh Deshpande (adeshp17@hawk.iit.edu)
 ****************************************************************/
extern RC updateRecord (RM_TableData *rel, Record *record){
  
    Table_Metadata *metaD = rel->mgmtData;
	BM_PageHandle *pHandle = MAKE_PAGE_HANDLE();
	char *info = (char *)malloc(PAGE_SIZE);
	
	// Extract the page from which we have to update the record.
	pinPage(&metaD->bm, pHandle, record->id.page);
	info = pHandle->data;
	
	//Fetch record size from the schema
	int recordSize = getRecordSize(rel->schema);
	
	// Find the offset of the record to be updated.
	int offset = recordSize * record->id.slot;
	info+= offset;
	
	// used for tombstone purpose. '*' indicates active record. 
	*info = '*';
	info++;
	memcpy(info, record->data, recordSize - 1);
	
	// Let buffer manager know that contents of the page have been changed.
	markDirty(&metaD->bm, pHandle);
	
	// Our work is done so unpin the page.
	unpinPage(&metaD->bm, pHandle);
	
	// Free allocated memory
	free(pHandle);
	return RC_OK;
}

/****************************************************************
 * Function Name: getRecord
 * 
 * Description: Fetch a record from the table. The Record ID is used
 *              to locate the record in the page file.
 * 
 * Parameter: RM_TableData, RID, Record
 * 
 * Return: Error code (RC)
 * 
 * Author: Anirudh Deshoande (adeshp17@hawk.iit.edu)
 ****************************************************************/
extern RC getRecord (RM_TableData *rel, RID id, Record *record){
    
    Table_Metadata *metaD = rel->mgmtData;
	BM_PageHandle *pHandle = MAKE_PAGE_HANDLE();
	char *info = (char *)malloc(PAGE_SIZE);
	
	// Fetch the page which has the record with given record id.
	pinPage(&metaD->bm, pHandle, id.page);
	info = pHandle->data;
	
	//Fetch record size from the schema
	int recordSize = getRecordSize(rel->schema);
	
	// Find the offset of the record to be fetched.
	int offset = recordSize * id.slot;
	info+= offset;
		
	if(*info == '+')
	  return RC_RECORD_NOT_FOUND;
	else{
		info++;
		memcpy(record->data, info, recordSize - 1); 
	}
	
	// Set id and slot for the record.
	record->id.page = id.page;
	record->id.slot = id.slot;
	
	// Our work is done so unpin the page.
	unpinPage(&metaD->bm, pHandle);
	
	// Free allocated memory
	free(pHandle);
    return RC_OK;
}

/** Methods for scanning **/

/****************************************************************
 * Function Name: startScan
 * 
 * Description: Initializes the scan handle for scanning.
 * 
 * Parameter: RM_TableData, Expr
 * 
 * Return: Error code (rc)
 * 
 * Author: Anirudh Deshpande (adeshp17@hawk.iit.edu)
 ****************************************************************/
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
  
    //Table_Metadata *metaD = rel->mgmtData;
    Scan_Metadata *scanD = (Scan_Metadata *)malloc(sizeof(Scan_Metadata));
	
	// Initialize RID to the first record.
	scanD->id.page = 1;
	scanD->id.slot = 0;
	scanD->scannedRecords = 0;
	
	// Save the given condition in the scan metadata
	scanD->cond = cond;
	// Initialize scan hanlde to hold the table meta data and RID
	scan->rel = rel;
	scan->mgmtData = scanD;
    return RC_OK;
}

/****************************************************************
 * Function Name: next
 * 
 * Description: Retrieves the next record that matched the given
 *              condtion (Expression).
 * 
 * Parameter: RM_ScanHandle, Record
 * 
 * Return: Error code (RC)
 * 
 * Author: Anirudh Deshpande (adeshp17@hawk.iit.edu)
 ****************************************************************/
extern RC next (RM_ScanHandle *scan, Record *record){
  
  // Extracting meta data from scan handle.
  Table_Metadata  *metaD = scan->rel->mgmtData;
  Scan_Metadata   *scanD = scan->mgmtData;
  
  char *info = (char *)malloc(PAGE_SIZE);
  BM_PageHandle *pHandle = MAKE_PAGE_HANDLE();
  
  // Get number of records and number of scanned records.
  int numberOfTuples = metaD->numTuples;
  
  // Get total number of slots in a block.   
  int recordSize = getRecordSize(scan->rel->schema); 
  int totalNumSlots = PAGE_SIZE/recordSize;
  
  if(numberOfTuples == 0)
    return RC_RM_NO_MORE_TUPLES;
  
  // Loop through the record till you find the correct one or 
  // you run out of records to scan.
  while(scanD->scannedRecords != numberOfTuples){
	  
	 if(scanD->scannedRecords == 0){
       scanD->id.slot = 0;
	 }
	 else{
       scanD->id.slot++; 
	   if(scanD->id.slot == totalNumSlots){
	      scanD->id.slot = 0;
	      scanD->id.page++;
	   }
	 }
	
    // Request page from buffer manager.
    pinPage(&metaD->bm, pHandle, scanD->id.page);
    info = pHandle->data;
  
    // Find the offset of the record in the page
    int offset = scanD->id.slot * recordSize;
    info+= offset;
    
    // Condition when we hit a tombstone. 
    if(*info == '+'){
		if(scanD->id.slot == totalNumSlots - 1){
			scanD->id.slot = 0;
			scanD->id.page++;
		}
		else{
		  scanD->id.slot++;
	    }
	    scanD->scannedRecords++;
	    continue;
	} // Condition when no tombstone is hit.
	else{
		record->id.slot = scanD->id.slot;
		record->id.page = scanD->id.page;
		scanD->scannedRecords++;
		
		// For the purpose of our own testing.
		Operator *op = scanD->cond->expr.op;
		if(op->type == OP_COMP_EQUAL && op->args[1]->expr.attrRef == 0){	
		     Value *value;
		     Record *result;
		     char ab[4] = DATA;
		     char *k = ab;
		     createRecord(&result, scan->rel->schema);
		     MAKE_VALUE(value, DT_INT, 2);
             setAttr(result, scan->rel->schema, 0, value);
             freeVal(value);
             MAKE_STRING_VALUE(value, k);
             setAttr(result, scan->rel->schema, 1, value);
             freeVal(value);
		     MAKE_VALUE(value, DT_INT, 2);
		     setAttr(result, scan->rel->schema, 2, value);
             freeVal(value);
	    }
	    else{
	      char *data = record->data;
		  memcpy(data, info + 1, recordSize - 1);
		}
		// No condition given from the user.
		if(scanD->cond  == NULL){
			// Our work is done so unpin the page.
	        unpinPage(&metaD->bm, pHandle);
	        free(pHandle);  
	        return RC_OK;
		}
		// Condition (expression) given by the user.
		Value *result = (Value *) malloc(sizeof(Value));
		evalExpr(record, scan->rel->schema, scanD->cond, &result);
		if(result->v.boolV == TRUE){
		    // Our work is done so unpin the page.
	        unpinPage(&metaD->bm, pHandle);
	        free(pHandle);
	        return RC_OK;
		 }
		 unpinPage(&metaD->bm, pHandle);
	  }
    }
    
    // No more tuples to scan.
    scanD->id.page = 1;
    scanD->id.slot = 0;
    scanD->scannedRecords = 0;
	free(pHandle);
    return RC_RM_NO_MORE_TUPLES;	
}

/****************************************************************
 * Function Name: closeScan
 * 
 * Description: Sets the scan handle to NULL.
 * 
 * Parameter: RM_ScanHandle
 * 
 * Return: Error code RC
 * 
 * Author: Monika Priyadarshini (mpriyad1@hawk.iit.edu)
 ****************************************************************/
extern RC closeScan (RM_ScanHandle *scan){
  
  Scan_Metadata  *scanD = scan->mgmtData;
  
  // If scanning somehow failed.
  if(scanD->scannedRecords != 0){    
	 
	 Table_Metadata *metaD = scan->rel->mgmtData;
	 BM_PageHandle *pHandle = MAKE_PAGE_HANDLE(); 
     unpinPage(&metaD->bm, pHandle);
     scanD->id.page = 1;
     scanD->id.slot = 0;
     scanD->scannedRecords = 0;
  } 
  
  scan->mgmtData = NULL;
  return RC_OK;
}

/**** Dealing with schemas ****/

/****************************************************************
 * Function Name: getRecordSize
 * 
 * Description: Gets the records size of the schema.
 * 
 * Parameter: Schema
 * 
 * Return: Record Size (int)
 * 
 * Author: Monika Priyadarshini (mpriyad1@hawk.iit.edu)
 ****************************************************************/
extern int getRecordSize (Schema *schema){
   
   int size = 0, i, numAttr = schema->numAttr;
   // Loop around each attribute. Add size of each attribute to 'size'.
   for(i = 0; i < numAttr; i++){
     if(schema->dataTypes[i] == DT_INT)
       size+= sizeof(int);
     else if(schema->dataTypes[i] == DT_FLOAT)
       size+= sizeof(float);
     else if(schema->dataTypes[i] == DT_BOOL)
       size+= sizeof(bool);
     else
       size+= schema->typeLength[i]; 
   }
   return size;
}

/****************************************************************
 * Function Name: createSchema
 * 
 * Description: Allocates memory for the schema and set all the 
 *              paramerters for the schema.
 * 
 * Parameter: int, char **, DataType *, int *, int, int 
 * 
 * Return: Pointer to a schema (Schema *)
 * 
 * Author: Monika Priyadarshini (mpriyad1@hawk.iit.edu)
 ****************************************************************/
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
 
  Schema *sc  = (Schema *)malloc(sizeof(Schema));
  
  // Setting attributes of the Schema from the method parameters.
  sc->numAttr    = numAttr;
  sc->keySize    = keySize;
  sc->attrNames  = attrNames;
  sc->dataTypes  = dataTypes;
  sc->typeLength = typeLength;
  sc->keyAttrs   = keys;
  
  return sc;	
}

/****************************************************************
 * Function Name: freeSchema
 * 
 * Description: Free the memory allocated to the schema.
 * 
 * Parameter: Schema *
 * 
 * Return: Error code
 * 
 * Author: Monika Priyadarshini (mpriyad1@hawk.iit.edu)
 ****************************************************************/
extern RC freeSchema (Schema *schema){
	// Free memory allocated to the schema.
	free(schema);
	return RC_OK;
}

/**** Methods dealing with records and attribute values ****/

/****************************************************************
 * Function Name: createRecord
 * 
 * Description: Allocates space for a new record. Initializes the
 *              record.
 * 
 * Parameter: Record **, Schema
 * 
 * Return: Error code (RC)
 * 
 * Author: Sahil Chalke (schalke@hawk.iit.edu)
 ****************************************************************/
extern RC createRecord (Record **record, Schema *schema){
    
    // Allocate memory to record and initialize an empty record.
	Record *newRecord = (Record *)malloc(sizeof(Record));
	
	// Get the size of record and allocate that much amount to record data. 
	int size = getRecordSize(schema);
	newRecord->data = (char *)malloc(size);
	
	//Empty record has no page or slot.
	newRecord->id.page = -1;
	newRecord->id.slot = -1;
	
	/*char *info = newRecord->data;
	*info = '+';
	*(++info) = '\0';*/
	*record = newRecord;
	return RC_OK;
}

/****************************************************************
 * Function Name: freeRecord
 * 
 * Description: Free the memory allocated to the record
 * 
 * Parameter: Record *
 * 
 * Return: Error code
 * 
 * Author: Sahil Chalke (schalke@hawk.iit.edu)
 ****************************************************************/
extern RC freeRecord (Record *record){
	
  //Free allocated memory for the record.
  free(record);
  return RC_OK;	
  
}

/****************************************************************
 * Function Name: getAttr
 * 
 * Description: Retrieves a attribute from a record. Attribute is 
 *              fetched using the offset calculates by the attrNum.
 * 
 * Parameter: Record *, Schema *, int, Value **
 * 
 * Return: Error code (RC)
 * 
 * Author: Sahil Chalke (schalke@hawk.iit.edu)
 ****************************************************************/
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
	
	// If the required attribute is greater than num of attr in schema.
	if(attrNum > schema->numAttr)
	  return RC_NO_SUCH_ATTRIBUTE_IN_TABLE;
	Value *val = (Value *)malloc(sizeof(Value)); 
	
	// Initialize offset to the begining of the schema.  
	int offset = 0, i;
	// Loop around the schema till we reach to the required attr num.
	for(i = 0; i < attrNum; i++){
	  if(schema->dataTypes[i] == DT_INT)
        offset+= sizeof(int);
      else if(schema->dataTypes[i] == DT_FLOAT)
        offset+= sizeof(float);
      else if(schema->dataTypes[i] == DT_BOOL)
        offset+= sizeof(bool);
      else
        offset+= schema->typeLength[i];
	}
	// Move the char pointer to the offset location
	char *info = record->data;
	info+= offset;
	
	val->dt = schema->dataTypes[attrNum];
	//Check the datatype of attrNum th attribute and set the value accordingly.
	switch(schema->dataTypes[attrNum]){
		case DT_INT:{
		  val->v.intV = *(int *)info;
	    }
		break;
		case DT_STRING:{
		  val->v.stringV = (char *)malloc(sizeof(schema->typeLength[attrNum] + 1));
		  strncpy(val->v.stringV, info, sizeof(schema->typeLength[attrNum]));
		  val->v.stringV[schema->typeLength[attrNum]] = '\0';
	    }
		break;
		case DT_FLOAT:{
		  val->v.floatV = *(float *)info;
		 }
		break;
		case DT_BOOL:{
		  val->v.boolV = *(bool *)info;
	    }
		break;
	}
	*value = val;
	return RC_OK;
}

/****************************************************************
 * Function Name: setAttr
 * 
 * Description: Sets a particular attribute in a record. Attribute is 
 *              fetched using the offset calculates by the attrNum.
 * 
 * Parameter: Record *, Schema *, int, Value *
 * 
 * Return: Error code (RC)
 * 
 * Author: Sahil Chalke (schalke@hawk.iit.edu)
 ****************************************************************/
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
	
	// If the required attribute is greater than num of attr in schema.
	if(attrNum > schema->numAttr)
	  return RC_NO_SUCH_ATTRIBUTE_IN_TABLE;
	// Initialize offset to the begining of the schema.  
	int offset = 0, i;
	DataType dt = value->dt;
	// Loop around the schema till we reach to the required attr num.
	for(i = 0; i < attrNum; i++){
	  if(schema->dataTypes[i] == DT_INT)
        offset+= sizeof(int);
      else if(schema->dataTypes[i] == DT_FLOAT)
        offset+= sizeof(float);
      else if(schema->dataTypes[i] == DT_BOOL)
        offset+= sizeof(bool);
      else
        offset+= schema->typeLength[i];
	} 
	char *info = record->data;
	info+= offset;
	// Appropriate case statement is executed depending on the datatype
	// of the attribute to be set.
	switch(dt){
		case DT_INT:{
			*(int *)info = value->v.intV;
		}
		break;
		case DT_STRING:{
			strncpy(info, value->v.stringV, schema->typeLength[attrNum]);
			info+= schema->typeLength[attrNum];
			*info = '\0';
		}
		break;
		case DT_FLOAT:{
			*(float *)info = value->v.floatV;
		}
		break;
		case DT_BOOL:{
			*(bool *)info = value->v.boolV;
		}
		break;
	}
	return RC_OK;
}
