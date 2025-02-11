import gc
import time
import traceback

from queue import Queue
from loguru import logger
from threading import Thread, Event, Lock
from typing import Generator, Iterator, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.utility.SdUtility import numProcess
from src.config import SdConfig
from src.controller.SdCtrl import SdController
from src.library.SdElastic import SdElasticConnect


class StreamingSplitDex(SdController):
    def __init__(self):
        super().__init__()
        self.logger = logger
        self.config = SdConfig
        self.es = None
        self.chunkQueue = Queue(maxsize=numProcess())
        self.stopEvent = Event()
        self.totalSuccess = 0
        self.totalFailed = 0
        self.connection_lock = Lock()


    def ensureConnectionES(self):
        """Ensures Elasticsearch connection is alive and reconnects if needed."""
        with self.connection_lock:
            try:
                if self.es is None:
                    self.es = SdElasticConnect(self.config)
                    self.es.connect()
                if not hasattr(self.es, '_client') or not self.es._client.ping():
                    self.logger.warning("Elasticsearch connection lost, reconnecting...")
                    self.es.connect()
            
            except Exception as e:
                self.logger.error(f"Error ensuring connection: {str(e)}")
                raise

    def chunkProducer(self, data: Generator[Any, None, None] | Iterator[Any]):
        """
        Produces chunks from a generator without loading all data into memory.
        Yields items one at a time from the generator and builds chunks progressively.
        """
        chunk = []
        count = 0
        
        try:
            self.logger.info("Producer: Starting to process data from generator")
            for item in data:
                if self.stopEvent.is_set():
                    self.logger.warning("Producer: Stop event detected, breaking loop")
                    break
                    
                chunk.append(item)
                count += 1
                
                if len(chunk) >= self.config.BATCH_SIZE:
                    self.logger.info(f"Producer: Created chunk of {len(chunk)} items. Total processed: {count}")
                    self.logger.debug(f"Producer: Queue size before put: {self.chunkQueue.qsize()}/{self.chunkQueue.maxsize}")
                    self.chunkQueue.put(chunk)
                    self.logger.debug(f"Producer: Successfully queued chunk. Queue size after: {self.chunkQueue.qsize()}/{self.chunkQueue.maxsize}")
                    chunk = []
                    gc.collect()
            
            if chunk and not self.stopEvent.is_set():
                self.logger.info(f"Producer: Created final chunk of {len(chunk)} items. Total processed: {count}")
                self.chunkQueue.put(chunk)

            self.logger.info("Producer: Finished processing all data")
        
        except Exception as e:
            self.logger.error(f"Producer: Error processing data: {str(e)}")
            self.logger.debug(traceback.format_exc())
            self.stop_event.set()
        finally:
            self.logger.info("Producer: Sending termination signal to consumers")
            for _ in range(numProcess()):  # Send termination signal for each consumer
                self.chunkQueue.put(None)
            del chunk
            gc.collect()
    
    def processAndIndexChunk(self, chunk: list):
        """Processes each chunk by mapping data for bulk indexing."""
        self.logger.info(f"Processing chunk with {len(chunk)} records.")
        if chunk is None:
            return None
        
        max_retries = self.config.MAX_RETRY_CONNECTION
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.ensureConnectionES()

                retry_count = 0

                actions = self.processChunk(chunk)
                success, failed = 0, 0

                if actions:
                    success, failed = self.es.bulkIndex(chunk, actions)
                
                del actions
                del chunk
                gc.collect()
                
                return success, failed
            except Exception as e:
                retry_count += 1
                self.logger.error(f"Error processing chunk (attempt {retry_count}/{max_retries}): {str(e)}")

                with self.connection_lock:
                    self.logger.info("Retrying chunk processing...")
                    # Force reconnection on next attempt
                    if self.es is not None:
                        try:
                            self.es._client.transport.close()
                        except:
                            pass
                    self.es = None

                if retry_count < max_retries:
                    time.sleep(2 ** retry_count)
                
        self.logger.error("Max retries reached, failing chunk")
        return 0, len(chunk)
    
    def chunkConsumer(self, consumer_id: int):
        """Consumes chunks from the queue and processes them."""
        self.logger.info(f"Consumer {consumer_id}: Started")
        while not self.stopEvent.is_set():
            try:
                self.logger.debug(f"Consumer {consumer_id}: Waiting for chunk. Queue size: {self.chunkQueue.qsize()}")
                chunk = self.chunkQueue.get()
                
                if chunk is None:  # End signal received
                    self.logger.info(f"Consumer {consumer_id}: Received termination signal")
                    self.chunkQueue.task_done()
                    break
                
                self.logger.info(f"Consumer {consumer_id}: Processing chunk of size {len(chunk)}")
                success, failed = self.processAndIndexChunk(chunk)
                self.totalSuccess += success
                self.totalFailed += failed
                
                self.logger.debug(f"Consumer {consumer_id}: Finished processing chunk. Success: {success}, Failed: {failed}")
                self.chunkQueue.task_done()
                del chunk  # Explicitly delete the processed chunk
                gc.collect()
            except Exception as e:
                self.logger.error(f"Consumer {consumer_id}: Error processing chunk: {str(e)}")
                self.logger.debug(traceback.format_exc())
                self.stopEvent.set()
                break

    def processChunk(self, chunk: list):
        """Processes each chunk by mapping data for bulk indexing."""
        self.logger.info(f"Processing chunk with {len(chunk)} records.")
        actions = []
        for item in chunk:
            try:
                mapped_data = self.mappingData(item)
                
                indexName = mapped_data.get("indexName")
                dataId = mapped_data.get("dataId")
                data = mapped_data.get("data")

                self.logger.info(f"""
                                Successfully mapped data:
                                Index: '{indexName}'
                                Id: '{dataId}'
                                DataLength: {len(str(data))} characters
                                """)
                
                action = {
                    "_index": indexName,
                    "_id": dataId,
                    "_source": data 
                }
                actions.append(action)
            except Exception as e:
                self.logger.error(f"Error mapping data: {str(e)}")
                self.logger.debug(traceback.format_exc())
                continue
        self.logger.info(f"Successfully processed {len(actions)} records in the chunk.")
        return actions
    
    def bulkIndexChunk(self, chunk):
        """Performs bulk indexing for a given chunk."""
        self.logger.info(f"Starting bulk indexing for {len(chunk)} records")
        try:
            actions = self.processChunk(chunk)
            
            if actions:
                self.logger.info(f"Generated {len(actions)} actions for bulk indexing")
                success, failed = self.es.bulkIndex(chunk, actions)
                self.logger.info(f"Bulk indexing result - Success: {success}, Failed: {failed}")
                return success, failed
            else:
                self.logger.warning("Skipping bulk indexing as no valid actions were generated.")
                return 0, len(chunk)
            
        except Exception as e:
            self.logger.error(f"Error in bulkIndexChunk: {str(e)}")
            self.logger.debug(traceback.format_exc())
            return 0, len(chunk)
    
    def action(self):
        """Main method to execute the bulk indexing process."""
        try:
            self.logger.info("Initializing Elasticsearch connection.")
            self.ensureConnectionES()
            
            self.logger.info("Getting data generator.")
            data_generator = self.getData()

            if not hasattr(data_generator, '__iter__') or not hasattr(data_generator, '__next__'):
                raise TypeError("getData() must return a generator or iterator")

            num_consumers = numProcess()
            self.logger.info(f"Starting {num_consumers} consumer threads")
            
            consumers = []
            with ThreadPoolExecutor(max_workers=num_consumers) as executor:
                consumers = [executor.submit(self.chunkConsumer, i) for i in range(num_consumers)]

                self.logger.info("Starting producer thread")
                producer = Thread(target=self.chunkProducer, args=(data_generator,))
                producer.start()

                # Wait for producer to finish
                producer.join()
                self.logger.info("Producer thread completed")
                
                # Wait for queue to be empty
                self.chunkQueue.join()
                self.logger.info("All chunks processed")
                
                # Wait for all consumers to complete
                for future in consumers:
                    future.result()

            self.logger.info(f"""
                Indexing process completed:
                Total Success: {self.totalSuccess}
                Total Failed: {self.totalFailed}
            """)

            return self.totalSuccess, self.totalFailed
        
        except Exception as e:
            self.logger.error(f"Critical error occurred during bulk indexing: {str(e)}")
            self.logger.debug(traceback.format_exc())
            self.stopEvent.set()  # Signal all threads to stop
            raise
        finally:
            # Safely close Elasticsearch connection
            with self.connection_lock:
                if self.es is not None:
                    try:
                        self.es._client.transport.close()
                    except:
                        pass
            gc.collect()
