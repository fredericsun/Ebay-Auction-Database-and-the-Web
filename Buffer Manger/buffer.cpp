/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

///////////////////////////////////////////////////////////////////////////////
//
// Title:            Buffer Manager
// Semester:         (CS564) Fall 2017
//
// Author:           Zhaoyin Qin, Hao Yuan, Yue Sun
// Email:            zqin23@wisc.edu, hyuan29@wisc.edu, ysun276@wisc.edu
//
///////////////////////////////////////////////////////////////////////////////

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

    BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	    bufDescTable = new BufDesc[bufs];

	    for (FrameId i = 0; i < bufs; i++) 
	    {
		bufDescTable[i].frameNo = i;
		bufDescTable[i].valid = false;
	    }

	    bufPool = new Page[bufs];

	    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
	    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

	    clockHand = bufs - 1;
	}


    BufMgr::~BufMgr() {
	for (std::uint32_t i = 0; i < numBufs; i++) {
	    if (bufDescTable[i].pinCnt != 0) {
		// If the page is pinned, throw an PagePinnedException.
		throw PagePinnedException(bufDescTable[i].file->filename(), bufDescTable[i].pageNo, i);
	    }
	}
	for (std::uint32_t i = 0; i < numBufs; i++) {
	    if (bufDescTable[i].dirty != 0) {
		// Write to disk if the page is dirty.
		bufDescTable[i].file->writePage(bufPool[i]);
	    }
	}
	delete[] bufDescTable;
	delete hashTable;
	delete[] bufPool;
    }

    void BufMgr::advanceClock() {
	// Increment the clockHand. If the value of clockHand is larger or
	// equal to the numBufs, the clockHand is reset to zero.
	if (++clockHand >= numBufs) {
	    clockHand = 0;
	}
    }

    void BufMgr::allocBuf(FrameId & frame) {
	// The number of pinned pages.
	std::uint32_t num = 0;
	// Keep looping if the number of pinned pages is less than the number
	// of frames.
	while (num < numBufs) {
	    advanceClock();
	    // If the page in this frame is not valid, assign its frame number
	    // to frame.
	    if (!bufDescTable[clockHand].valid) { 
		frame = bufDescTable[clockHand].frameNo;
		return;
	    }
	    if (bufDescTable[clockHand].refbit) {
		bufDescTable[clockHand].refbit = false;
		continue;
	    }
	    if (bufDescTable[clockHand].pinCnt != 0) {
		// Increment the number of pinned pages.
		num++;
		continue;
	    } else {
		// Flush the page to disk.
		if (bufDescTable[clockHand].dirty) {
		    bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
                    bufStats.diskwrites++;
                    bufStats.accesses++;
		}
		frame = bufDescTable[clockHand].frameNo;
		// Remove the entry of corresponding page from the hash table.   
		hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
		bufDescTable[clockHand].Clear();
		return;
	    }           
	}
	// All the pages are pinned.
	throw BufferExceededException();
    }


    void BufMgr::readPage(File* file, const PageId pageNo, Page*& page) {
	// *&: a reference to a pointer to a Page.
	FrameId frameNo;
	try {
	    // Check whether the page is already in the buffer pool. 
	    hashTable->lookup(file, pageNo, frameNo);
	    bufDescTable[frameNo].refbit = true;
	    bufDescTable[frameNo].pinCnt++;
	    page = &bufPool[frameNo];
            bufStats.accesses++;
	} catch (HashNotFoundException& e) {
	    // Allocate a buffer frame.
	    allocBuf(frameNo);
	    // Read the page from the disk to the buffer pool frame.
	    bufPool[frameNo] = file->readPage(pageNo);
            bufStats.accesses++;
            bufStats.diskreads++;
	    // Insert the page into the hashtable.
	    hashTable->insert(file, pageNo, frameNo);
	    // Set up the frame properly.
	    bufDescTable[frameNo].Set(file, pageNo);
	    page = &bufPool[frameNo];
            bufStats.accesses++;
	} catch (BufferExceededException& e) {
	    /* All the pages are pinned currently */
	}
    }


    void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) {
	FrameId frameNo;
	try {
	    // Find the frame containing file and pageNo.
	    hashTable->lookup(file, pageNo, frameNo);
	    if (bufDescTable[frameNo].pinCnt == 0) {
		// The pin count of this frame is already zero.
		throw PageNotPinnedException(bufDescTable[frameNo].file->filename(), pageNo, frameNo);
	    } else {
		// Decrement pin count.
		bufDescTable[frameNo].pinCnt--;
	    }
	    if (dirty) {
		bufDescTable[frameNo].dirty = true;
	    }
	} catch (HashNotFoundException& e) {
	    /* Do nothing if page is not found in the hash table lookup. */
	}
    }

    void BufMgr::flushFile(const File* file) {
	for (std::uint32_t i = 0; i < numBufs; i++) {
	    if (bufDescTable[i].file == file) {
		// The page is pinned.
		if (bufDescTable[i].pinCnt != 0) {
		    throw PagePinnedException(bufDescTable[i].file->filename(), bufDescTable[i].pageNo, i);
		}
		// The page is not valid.
		if (!bufDescTable[i].valid) {
		    throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
		}
	    }
	}
	// Scan bufDescTable for pages belonging to the file.
	for (std::uint32_t i = 0; i < numBufs; i++) {
	    if (bufDescTable[i].file == file) {
		// Flush the page to disk if the page is dirty.
		if (bufDescTable[i].dirty) {
		    bufDescTable[i].file->writePage(bufPool[i]);
                    bufStats.accesses++;
                    bufStats.diskwrites++;
		    bufDescTable[i].dirty = false;
		}
		// Remove the corresponding entry from the hash table.
		hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo);
		bufDescTable[i].Clear();
	    }
	}               
    }

    void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) {
	FrameId frameNo;
	allocBuf(frameNo);
	// Allocate an empty page.
	bufPool[frameNo] = file->allocatePage();
        bufStats.accesses++;
	page = &bufPool[frameNo];
        bufStats.accesses++;
	// Insert the corresponding entry to the hash table.
	pageNo = bufPool[frameNo].page_number();
        bufStats.accesses++;
        hashTable->insert(file, pageNo, frameNo);
	bufDescTable[frameNo].Set(file, pageNo);
    }

    void BufMgr::disposePage(File* file, const PageId PageNo) {
	FrameId frameNo;
	try {
	    // Check if the page to be deleted is in the buffer pool.
	    hashTable->lookup(file, PageNo, frameNo);
	    if (bufDescTable[frameNo].pinCnt != 0) {
		throw PagePinnedException(bufDescTable[frameNo].file->filename(), bufDescTable[frameNo].pageNo, frameNo);
	    }
	    // Free the frame.
	    bufDescTable[frameNo].Clear();
	    // Delete the corresponding entry from the hashtable.
	    hashTable->remove(file, PageNo);
	} catch (HashNotFoundException& e) {
	    /* Do nothing */
	}
	// Delete the page from the file.
	file->deletePage(PageNo);
    }

    void BufMgr::printSelf(void) 
    {
	BufDesc* tmpbuf;
	int validFrames = 0;

	for (std::uint32_t i = 0; i < numBufs; i++)
	{
	    tmpbuf = &(bufDescTable[i]);
	    std::cout << "FrameNo:" << i << " ";
	    tmpbuf->Print();

	    if (tmpbuf->valid == true)
		validFrames++;
	}

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
    }

}
