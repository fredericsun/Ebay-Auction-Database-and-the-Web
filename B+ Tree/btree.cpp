/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
    std::ostringstream idxStr;
    // Construct index file name.
    idxStr << relationName << '.' << attrByteOffset;
    // Return the name of the index file.
    outIndexName = idxStr.str(); 

    this->bufMgr =  bufMgrIn;
    this->attrByteOffset = attrByteOffset;
    this->attributeType = attrType;
    // Number of keys in leaf node.
    this->leafOccupancy = INTARRAYLEAFSIZE;
    // Number of keys in non-leaf node.
    this->nodeOccupancy = INTARRAYNONLEAFSIZE;

    // The constructor needs to check if the specified index file exists.
    if (std::ifstream(outIndexName)) {
        // The file exists.
        this->file = new BlobFile(outIndexName, false);
        // The page number of meta page is the pageid of first page in the file.
        this->headerPageNum = file->getFirstPageNo();
        // The meta page.
        Page *headerInfoPage;
        // Reads the meta page from the file.
        this->bufMgr->readPage(this->file, this->headerPageNum, headerInfoPage);
        // Cast headerInfoPage to struct IndexMetaInfo to retrieve information.
        IndexMetaInfo *indexMetaInfo = (IndexMetaInfo*)headerInfoPage;
        // Get page number of root page of the B+ Tree inside the file index file.
        this->rootPageNum = indexMetaInfo->rootPageNo;
        // Check if the values in meta page match with values received through
        // constructor parameters.
        if (this->attrByteOffset != indexMetaInfo->attrByteOffset) {
            throw BadIndexInfoException("The offset of attribute is not consistent.");
        }
        if (this->attributeType != indexMetaInfo->attrType) {
            throw BadIndexInfoException("The datatype of attribute is not consistent.");
        }
        if (relationName != indexMetaInfo->relationName) {
            throw BadIndexInfoException("The name of base relation is not consistent.");
        }
        // readPage() increases the pinCnt of the page. The page has not been modified.
        this->bufMgr->unPinPage(this->file, this->headerPageNum, false);
    } else {
        // The file does not exist.
        this->file = new BlobFile(outIndexName, true);
        Page *headerInfoPage;
        // Allocate a new page for meta data in the file. The pointer to that page is stored
        // in headerInfoPage (allocPage() set pinCnt to 1).
        this->bufMgr->allocPage(this->file, this->headerPageNum, headerInfoPage);
        Page *rootNodePage;
        // Allocate a new page for the root node. The pointer to that page is stored
        // in rootNodePage.
        this->bufMgr->allocPage(this->file, this->rootPageNum, rootNodePage);
        IndexMetaInfo *indexMetaInfo = (IndexMetaInfo*)headerInfoPage;
        // Copy the information to struct IndexMetaInfo.
        std::size_t length = relationName.copy(indexMetaInfo->relationName,
                                               relationName.length(), 0);
        indexMetaInfo->relationName[length] = '\0';
        indexMetaInfo->attrByteOffset = this->attrByteOffset;
        indexMetaInfo->attrType = this->attributeType;
        indexMetaInfo->rootPageNo = this->rootPageNum;
        // The root node.
        LeafNodeInt *root = (LeafNodeInt*)rootNodePage;
        // Number of keys in the root node.
        root->num = 0;
        // The root does not have a rightSibPageNo currently.
        root->rightSibPageNo = 0;
        this->bufMgr->unPinPage(this->file, this->headerPageNum, true);
        this->bufMgr->unPinPage(this->file, this->rootPageNum, true);
        // Scan the records and insert them into the B+ tree.
        FileScan *fs = new FileScan(relationName, this->bufMgr);
        try {
            RecordId recordId; 
            for (;;) {
                // scanNext() may throw EndOfFileException.
                fs->scanNext(recordId);
                void *key = (void*)(fs->getRecord().c_str() + attrByteOffset);
                insertEntry(key, recordId);
            }
        } catch (EndOfFileException& e) {
            /* File scan completed. */
        }
        this->bufMgr->flushFile(file);
        delete(fs);
    }    
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    // Flush index file.
    this->bufMgr->flushFile(this->file);
    // End initialized scan.
    this->scanExecuting = false;
    // Delete file instance.
    delete(this->file);
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
     RIDKeyPair<int> data;
     data.set(rid, *((int *)key));
     // root
     Page* root;
     // PageId rootPageNum;
     bufMgr->readPage(file, rootPageNum, root);
     PageKeyPair<int> *child = nullptr;
     insert(root, rootPageNum, origRootPageNum == rootPageNum ? true : false, data, child);
}

/**
 * Recursive function to insert the index entry to the index file
 * @param curPage
 * @param curPageNum        
 * @param nodeIsLeaf     
 * @param data      
 * @param child     
*/
const void BTreeIndex::insert(Page *curPage, PageId curPageNum, bool nodeIsLeaf, const RIDKeyPair<int> data, PageKeyPair<int> *&child)
{
	if (nodeIsLeaf){
		LeafNodeInt *leaf = (LeafNodeInt *)curPage;
		if (leaf->ridArray[leafOccupancy - 1].page_number == 0)
		{
		  insertLeaf(leaf, data);
		  bufMgr->unPinPage(file, curPageNum, true);
		  child = nullptr;
		}
		else
		{
		  splitLeaf(leaf, curPageNum, child, data);
		}
	}
	else
	{
		NonLeafNodeInt *curNode = (NonLeafNodeInt *)curPage;
		// find the right key to traverse
		Page *nextPage;
		PageId nextNodePageNum;
		int i = nodeOccupancy;
		while(i >= 0 && (curNode->pageNoArray[i] == 0))
		{
			i--;
		}
		while(i > 0 && (curNode->keyArray[i-1] >= data.key))
		{
			i--;
		}
		nextNodePageNum = curNode->pageNoArray[i];
		bufMgr->readPage(file, nextNodePageNum, nextPage);
		// NonLeafNodeInt *nextNode = (NonLeafNodeInt *)nextPage;
		nodeIsLeaf = curNode->level == 1;
		insert(nextPage, nextNodePageNum, nodeIsLeaf, data, child);
		// no split in child, just return
		if (child == nullptr)
			{
				// unpin current page from call stack
				bufMgr->unPinPage(file, curPageNum, false);
			}
			else
			{ 
			  // if the curpage is not full
			  if (curNode->pageNoArray[nodeOccupancy] == 0)
			  {
				// insert the child to curpage
				insertNonLeaf(curNode, child);
				child = nullptr;
				// finish the insert process, unpin current page
				bufMgr->unPinPage(file, curPageNum, true);
			  }
			  else
			  {
				splitNonLeaf(curNode, curPageNum, child);
			  }
			}
    }
}

const void BTreeIndex::insertLeaf(LeafNodeInt *leaf, RIDKeyPair<int> data)
{
  // Insert directly if leaf is empty
  if (leaf->ridArray[0].page_number == 0)
  {
    leaf->keyArray[0] = data.key;
    leaf->ridArray[0] = data.rid;    
  }
  else
  {
    int i = leafOccupancy - 1;
    // find the index of the array where it is not occupied
    while(i >= 0 && (leaf->ridArray[i].page_number == 0))
    {
      i--;
    }
    // shift entries in the array to the right if the data to be inserted is smaller
    while(i >= 0 && (leaf->keyArray[i] > data.key))
    {
      leaf->keyArray[i+1] = leaf->keyArray[i];
      leaf->ridArray[i+1] = leaf->ridArray[i];
      i--;
    }
    // insert the data to fill the gap
    leaf->keyArray[i+1] = data.key;
    leaf->ridArray[i+1] = data.rid;
  }
}

const void BTreeIndex::splitLeaf(LeafNodeInt *leaf, PageId leafPageNum, PageKeyPair<int> *&child, const RIDKeyPair<int> data)
{
  // Create a new leaf node
  Page *newPage;
  PageId newPageNum;
  bufMgr->allocPage(file, newPageNum, newPage);
  LeafNodeInt *newLeafNode = (LeafNodeInt *)newPage;

  int midval = leafOccupancy/2;
  // odd number of keys
  if (leafOccupancy %2 == 1 && data.key > leaf->keyArray[midval])
  {
    midval = midval + 1;
  }
  // copy half the page to the newLeafNode
  for(int i = midval; i < leafOccupancy; i++)
  {
    newLeafNode->keyArray[i-midval] = leaf->keyArray[i];
    newLeafNode->ridArray[i-midval] = leaf->ridArray[i];
    leaf->keyArray[i] = 0;
    leaf->ridArray[i].page_number = 0;
  }
  
  if (data.key > leaf->keyArray[midval-1])
  {
    insertLeaf(newLeafNode, data);
  }
  else
  {
    insertLeaf(leaf, data);
  }

  // update sibling pointer
  newLeafNode->rightSibPageNo = leaf->rightSibPageNo;
  leaf->rightSibPageNo = newPageNum;

  // set the smallest key from right page as the new child entry
  child = new PageKeyPair<int>();
  PageKeyPair<int> newKeyPair;
  newKeyPair.set(newPageNum, newLeafNode->keyArray[0]);
  child = &newKeyPair;
  bufMgr->unPinPage(file, leafPageNum, true);
  bufMgr->unPinPage(file, newPageNum, true);

  // if curr page is root
  if (leafPageNum == rootPageNum)
  {
	// create a new root 
	PageId newRootPageNum;
	Page *newRoot;
	bufMgr->allocPage(file, newRootPageNum, newRoot);
	NonLeafNodeInt *newRootPage = (NonLeafNodeInt *)newRoot;

	// update new root info
	newRootPage->level = origRootPageNum == rootPageNum ? 1 : 0;
	newRootPage->keyArray[0] = child->key;
	newRootPage->pageNoArray[0] = leafPageNum;
	newRootPage->pageNoArray[1] = child->pageNo;
	// create meta
	Page *meta;
	bufMgr->readPage(file, headerPageNum, meta);
	IndexMetaInfo *metaPage = (IndexMetaInfo *)meta;
	metaPage->rootPageNo = newRootPageNum;
	rootPageNum = newRootPageNum;
	// unpin unused page
	bufMgr->unPinPage(file, headerPageNum, true);
	bufMgr->unPinPage(file, newRootPageNum, true);
  }
}

const void BTreeIndex::insertNonLeaf(NonLeafNodeInt *nonleaf, PageKeyPair<int> *data)
{
  
  int i = nodeOccupancy;
  while(i >= 0 && (nonleaf->pageNoArray[i] == 0))
  {
    i--;
  }
  while( i > 0 && (nonleaf->keyArray[i-1] > data->key))
  {
	nonleaf->pageNoArray[i+1] = nonleaf->pageNoArray[i];
    nonleaf->keyArray[i] = nonleaf->keyArray[i-1];
    i--;
  }
  // insert the non leaf node
  nonleaf->keyArray[i] = data->key;
  nonleaf->pageNoArray[i+1] = data->pageNo;
}

const void BTreeIndex::splitNonLeaf(NonLeafNodeInt *curNode, PageId curPageNum, PageKeyPair<int> *&child)
{
  // allocate a new nonleaf node
  PageId newPageNum;
  Page *newPage;
  bufMgr->allocPage(file, newPageNum, newPage);
  NonLeafNodeInt *newNode = (NonLeafNodeInt *)newPage;

  int mid = nodeOccupancy/2;
  int pushupIndex = mid;
  PageKeyPair<int> pushupEntry;
  if (nodeOccupancy % 2 == 0)
  {
    pushupIndex = child->key < curNode->keyArray[mid] ? mid -1 : mid;
  }
  pushupEntry.set(newPageNum, curNode->keyArray[pushupIndex]);

  mid = pushupIndex + 1;
  // move half the entries to the new node
  for(int i = mid; i < nodeOccupancy; i++)
  {
    newNode->keyArray[i-mid] = curNode->keyArray[i];
    newNode->pageNoArray[i-mid] = curNode->pageNoArray[i+1];
    curNode->pageNoArray[i+1] = 0;
    curNode->keyArray[i+1] = 0;
  }

  newNode->level = curNode->level;
  // remove the part that is pushed up from current node
  curNode->keyArray[pushupIndex] = 0;
  curNode->pageNoArray[pushupIndex] = 0;
  // insert the new child 
  insertNonLeaf(child->key < newNode->keyArray[0] ? curNode : newNode, child);
  child = &pushupEntry;
  bufMgr->unPinPage(file, curPageNum, true);
  bufMgr->unPinPage(file, newPageNum, true);

  // if the curNode is the root
  if (curPageNum == rootPageNum)
  {
	// create a new root 
	PageId newRootPageNum;
	Page *newRoot;
	bufMgr->allocPage(file, newRootPageNum, newRoot);
	NonLeafNodeInt *newRootPage = (NonLeafNodeInt *)newRoot;

	// update metadata
	newRootPage->level = origRootPageNum == rootPageNum ? 1 : 0;
	newRootPage->keyArray[0] = child->key;
	newRootPage->pageNoArray[0] = curPageNum;
	newRootPage->pageNoArray[1] = child->pageNo;
	// create meta
	Page *meta;
	bufMgr->readPage(file, headerPageNum, meta);
	IndexMetaInfo *metaPage = (IndexMetaInfo *)meta;
	metaPage->rootPageNo = newRootPageNum;
	rootPageNum = newRootPageNum;
	// unpin unused page
	bufMgr->unPinPage(file, headerPageNum, true);
	bufMgr->unPinPage(file, newRootPageNum, true);
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    int *low = (int*)lowValParm;
    int *high = (int*)highValParm;
    // Check if lower bound is larger than higher bound.
    if ((*low) > (*high)) {
        throw BadScanrangeException();
    }
    // Check if the scan operations are valid. 
    if (lowOpParm != GT || lowOpParm != GTE || highOpParm != LT || highOpParm != LTE) {
        throw BadOpcodesException();
    }
    this->lowOp = lowOpParm;
    this->highOp = highOpParm;
    
    if (this->lowOp == GT) {
        // Make the lower bound inclusive.
        this->lowValInt = *low + 1;
    }
    this->lowValInt = *low;
    if (this->highOp == LT) {
        // Make the higher bound inclusive.
        this->highValInt = *high + 1;
    }
    this->highValInt = *high;

    // If another scan has been started, that needs to be ended.
    if (this->scanExecuting) {
        endScan();
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
    // Check if scan has been initialized.
    if (!this->scanExecuting) {
        throw ScanNotInitializedException();
    }
    // Get current page being scanned.
    LeafNodeInt *curr = (LeafNodeInt*) this->currentPageData;
    // Check if the index of the entry to be scanned is larger or equal to the
    // number of entries (keys) in the array.
    if (this->nextEntry >= curr->num) {
        // Check if there are more leaf nodes.
        if (curr->rightSibPageNo == 0) {
            // Unpin the current page and throw the exception.
            this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
            throw IndexScanCompletedException();
        } else {
            // A rightSibPageNo exists.
            this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
            // Go the right leaf node and get its page number.
            this->currentPageNum = curr->rightSibPageNo;
            // Update the currentPageData to the current page being scanned.
            this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
            // Update curr pointer to the new leaf node.
            curr = (LeafNodeInt*) this->currentPageData;
            // Set value of newEntry to 0.
            this->nextEntry = 0;
        }
    }
    if (curr->keyArray[this->nextEntry] <= this->highValInt) {
        outRid = curr->ridArray[this->nextEntry];
        this->nextEntry++;
    } else {
        throw IndexScanCompletedException();
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
    // Check if scan has been initialized.
    if (!this->scanExecuting) {
        throw ScanNotInitializedException();
    }
    // Reset variables.
    this->currentPageNum = 0;
    this->currentPageData = NULL;
    this->nextEntry = 0;
    this->scanExecuting = false;
    this->lowValInt = 0;
    this->highValInt = 0;
    // Unpin the current page being scanned.
    this->bufMgr->unPinPage(this->file, this->currentPageNum, false);
}

}
