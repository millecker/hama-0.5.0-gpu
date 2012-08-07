/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "hama/Pipes.hh"
#include "hadoop/SerialUtils.hh"
#include "hadoop/StringUtils.hh"

#include <map>
#include <vector>

#include <errno.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <pthread.h>
#include <iostream>
#include <fstream>

#include <openssl/hmac.h>
#include <openssl/buffer.h>

using std::map;
using std::string;
using std::vector;

using namespace HadoopUtils;

namespace HamaPipes {

  /********************************************/
  /****************** BSPJob ******************/  
  /********************************************/
  class BSPJobImpl: public BSPJob {
  private:
    map<string, string> values;
  public:
    void set(const string& key, const string& value) {
      values[key] = value;
    }

    virtual bool hasKey(const string& key) const {
      return values.find(key) != values.end();
    }

    virtual const string& get(const string& key) const {
      map<string,string>::const_iterator itr = values.find(key);
      if (itr == values.end()) {
        throw Error("Key " + key + " not found in BSPJob");
      }
      return itr->second;
    }

    virtual int getInt(const string& key) const {
      const string& val = get(key);
      return toInt(val);
    }

    virtual float getFloat(const string& key) const {
      const string& val = get(key);
      return toFloat(val);
    }

    virtual bool getBoolean(const string&key) const {
      const string& val = get(key);
      return toBool(val);
    }
  };
    
  /********************************************/
  /************* DownwardProtocol *************/  
  /********************************************/
  class DownwardProtocol {
  public:
    virtual void start(int protocol) = 0;
    virtual void setBSPJob(vector<string> values) = 0;
    virtual void setInputTypes(string keyType, string valueType) = 0;
    virtual void setKeyValue(const string& _key, const string& _value) = 0;
      
    virtual void runBsp(bool pipedInput, bool pipedOutput) = 0;
    virtual void runCleanup(bool pipedInput, bool pipedOutput) = 0;
    virtual void runSetup(bool pipedInput, bool pipedOutput) = 0;
      
    virtual void setNewResult(int value) = 0;
    virtual void setNewResult(const string&  value) = 0;
    virtual void setNewResult(vector<string> value) = 0;
      
    //virtual void reduceKey(const string& key) = 0;
    //virtual void reduceValue(const string& value) = 0;
    virtual void close() = 0;
    virtual void abort() = 0;
    virtual ~DownwardProtocol() {}
  };

  /********************************************/
  /************** UpwardProtocol **************/  
  /********************************************/
  class UpwardProtocol {
  public:
      
    virtual void sendMessage(const string& peerName, const string& msg);
    virtual void getMessage() = 0;
    virtual void getMessageCount() = 0;
    virtual void sync() = 0;
    //virtual void getSuperstepCount() = 0;
    virtual void getPeerName(int index) = 0;
    //virtual void getPeerIndex() = 0;
    virtual void getAllPeerNames() = 0;
    //virtual void getNumPeers() = 0;
    //virtual void clear() = 0;
    virtual void writeKeyValue(const string& key, const string& value) = 0; 
    virtual void readNext() = 0;
    virtual void reopenInput() = 0;
    virtual void done() = 0;
    
    //virtual void registerCounter(int id, const string& group, const string& name) = 0;
    //virtual void incrementCounter(const TaskContext::Counter* counter, uint64_t amount) = 0;
    virtual void incrementCounter(const string& group, const string& name, uint64_t amount) = 0;
    virtual ~UpwardProtocol() {}
  };
    
  /********************************************/
  /***************** Protocol *****************/  
  /********************************************/
  class Protocol {
  public:
    virtual void nextEvent() = 0;
    virtual UpwardProtocol* getUplink() = 0;
    virtual ~Protocol() {}
  };
    
  /********************************************/
  /*************** MESSAGE_TYPE ***************/  
  /********************************************/
  enum MESSAGE_TYPE {START_MESSAGE, SET_BSPJOB_CONF, SET_INPUT_TYPES,       
      RUN_SETUP, RUN_BSP, RUN_CLEANUP,
      READ_KEYVALUE, WRITE_KEYVALUE, GET_MSG, GET_MSG_COUNT, 
      SEND_MSG, SYNC, 
      GET_ALL_PEERNAME, GET_PEERNAME,
      REOPEN_INPUT,
                     
      CLOSE=47, ABORT,
      DONE, REGISTER_COUNTER, INCREMENT_COUNTER};
    

  /********************************************/
  /*********** BinaryUpwardProtocol ***********/  
  /********************************************/
  class BinaryUpwardProtocol: public UpwardProtocol {
  private:
    FileOutStream* stream;
  public:
    BinaryUpwardProtocol(FILE* _stream) {
      stream = new FileOutStream();
      HADOOP_ASSERT(stream->open(_stream), "problem opening stream");
    }

    /*
    virtual void authenticate(const string &responseDigest) {
      serializeInt(AUTHENTICATION_RESP, *stream);
      serializeString(responseDigest, *stream);
      stream->flush();
    }
    */

    virtual void sendMessage(const string& peerName, const string& msg) {
      serializeInt(SEND_MSG, *stream);
      serializeString(peerName, *stream);
      serializeInt(msg.size(), *stream);
      serializeString(msg, *stream);
      stream->flush();
    }
    virtual void getMessage(){
      serializeInt(GET_MSG, *stream);
    } 
    virtual void getMessageCount(){
      serializeInt(GET_MSG_COUNT, *stream);
    } 
    virtual void sync(){
      serializeInt(SYNC, *stream);
    } 
    //virtual void getSuperstepCount() = 0;
    
    virtual void getPeerName(int index){
      serializeInt(GET_PEERNAME, *stream);
      serializeInt(index, *stream);
    } 
    //virtual void getPeerIndex() = 0;
    virtual void getAllPeerNames(){
      serializeInt(GET_ALL_PEERNAME, *stream);
    } 

    //virtual void getNumPeers() = 0;
    //virtual void clear() = 0;
     
    virtual void writeKeyValue(const string& key, const string& value) {
      serializeInt(WRITE_KEYVALUE, *stream);
      serializeString(key, *stream);
      serializeString(value, *stream);
      stream->flush();
    }
      
    virtual void readNext() {
        serializeInt(READ_KEYVALUE, *stream);
    }
      
    virtual void reopenInput() {
        serializeInt(REOPEN_INPUT, *stream);
    }
    virtual void done() {
      serializeInt(DONE, *stream);
    }

    /*
    virtual void registerCounter(int id, const string& group, 
                                 const string& name) {
      serializeInt(REGISTER_COUNTER, *stream);
      serializeInt(id, *stream);
      serializeString(group, *stream);
      serializeString(name, *stream);
    }
    virtual void incrementCounter(const TaskContext::Counter* counter, 
                                  uint64_t amount) {
      serializeInt(INCREMENT_COUNTER, *stream);
      serializeInt(counter->getId(), *stream);
      serializeLong(amount, *stream);
    }
     */
    virtual void incrementCounter(const string& group, const string& name, uint64_t amount) {
        serializeInt(INCREMENT_COUNTER, *stream);
        serializeString(group, *stream);
        serializeString(name, *stream);
        serializeLong(amount, *stream);
    }
      
    ~BinaryUpwardProtocol() {
      delete stream;
    }
  };

  /********************************************/
  /************** BinaryProtocol **************/  
  /********************************************/
  class BinaryProtocol: public Protocol {
  private:
    FileInStream* downStream;
    DownwardProtocol* handler;
    BinaryUpwardProtocol * uplink;
      
    string key;
    string value;
    /*
    string password;
    bool authDone;
      
    void getPassword(string &password) {
      const char *passwordFile = getenv("hama.pipes.shared.secret.location");
      if (passwordFile == NULL) {
        return;
      }
      std::ifstream fstr(passwordFile, std::fstream::binary);
      if (fstr.fail()) {
        std::cerr << "Could not open the password file" << std::endl;
        return;
      } 
      unsigned char * passBuff = new unsigned char [512];
      fstr.read((char *)passBuff, 512);
      int passwordLength = fstr.gcount();
      fstr.close();
      passBuff[passwordLength] = 0;
      password.replace(0, passwordLength, (const char *) passBuff, passwordLength);
      delete [] passBuff;
      return; 
    }
    
    
    void verifyDigestAndRespond(string& digest, string& challenge) {
      if (password.empty()) {
        //password can be empty if process is running in debug mode from
        //command file.
        authDone = true;
        return;
      }

      if (!verifyDigest(password, digest, challenge)) {
        std::cerr << "Server failed to authenticate. Exiting" << std::endl;
        exit(-1);
      }
      authDone = true;
      string responseDigest = createDigest(password, digest);
      uplink->authenticate(responseDigest);
    }

    bool verifyDigest(string &password, string& digest, string& challenge) {
      string expectedDigest = createDigest(password, challenge);
      if (digest == expectedDigest) {
        return true;
      } else {
        return false;
      }
    }


    string createDigest(string &password, string& msg) {
      HMAC_CTX ctx;
      unsigned char digest[EVP_MAX_MD_SIZE];
      HMAC_Init(&ctx, (const unsigned char *)password.c_str(), 
          password.length(), EVP_sha1());
      HMAC_Update(&ctx, (const unsigned char *)msg.c_str(), msg.length());
      unsigned int digestLen;
      HMAC_Final(&ctx, digest, &digestLen);
      HMAC_cleanup(&ctx);

      //now apply base64 encoding
      BIO *bmem, *b64;
      BUF_MEM *bptr;

      b64 = BIO_new(BIO_f_base64());
      bmem = BIO_new(BIO_s_mem());
      b64 = BIO_push(b64, bmem);
      BIO_write(b64, digest, digestLen);
      BIO_flush(b64);
      BIO_get_mem_ptr(b64, &bptr);

      char digestBuffer[bptr->length];
      memcpy(digestBuffer, bptr->data, bptr->length-1);
      digestBuffer[bptr->length-1] = 0;
      BIO_free_all(b64);

      return string(digestBuffer);
    }
    */

  public:
    BinaryProtocol(FILE* down, DownwardProtocol* _handler, FILE* up) {
      downStream = new FileInStream();
      downStream->open(down);
      uplink = new BinaryUpwardProtocol(up);
      handler = _handler;
      
      //authDone = false;
      //getPassword(password);
    }

    UpwardProtocol* getUplink() {
      return uplink;
    }

      
    virtual void nextEvent() {
      int32_t cmd;
      cmd = deserializeInt(*downStream);
      
      /*
      if (!authDone && cmd != AUTHENTICATION_REQ) {
        //Authentication request must be the first message if
        //authentication is not complete
        std::cerr << "Command:" << cmd << "received before authentication. " 
            << "Exiting.." << std::endl;
        exit(-1);
      }*/
    
      switch (cmd) {
              
      /*case AUTHENTICATION_REQ: {
        string digest;
        string challenge;
        deserializeString(digest, *downStream);
        deserializeString(challenge, *downStream);
        verifyDigestAndRespond(digest, challenge);
        break;
      }*/
              
      case START_MESSAGE: {
        int32_t prot;
        prot = deserializeInt(*downStream);
        handler->start(prot);
        break;
      }
      case SET_BSPJOB_CONF: {
        int32_t entries;
        entries = deserializeInt(*downStream);
        vector<string> result(entries);
        for(int i=0; i < entries; ++i) {
          string item;
          deserializeString(item, *downStream);
          result.push_back(item);
        }
        handler->setBSPJob(result);
        break;
      }
      case SET_INPUT_TYPES: {
        string keyType;
        string valueType;
        deserializeString(keyType, *downStream);
        deserializeString(valueType, *downStream);
        handler->setInputTypes(keyType, valueType);
        break;
      }
      case READ_KEYVALUE: {
        deserializeString(key, *downStream);
        deserializeString(value, *downStream);
        handler->setKeyValue(key, value);
        break;
      }
      case RUN_SETUP: {
        //string split;
        int32_t pipedInput;
        int32_t pipedOutput;
        //deserializeString(split, *downStream);
        pipedInput = deserializeInt(*downStream);
        pipedOutput = deserializeInt(*downStream);
        handler->runSetup(pipedInput, pipedOutput);
        break;
      }
      case RUN_BSP: {
        //string split;
        int32_t pipedInput;
        int32_t pipedOutput;
        //deserializeString(split, *downStream);
        pipedInput = deserializeInt(*downStream);
        pipedOutput = deserializeInt(*downStream);
        handler->runBsp(pipedInput, pipedOutput);
        break;
      }
      case RUN_CLEANUP: {
        //string split;
        int32_t pipedInput;
        int32_t pipedOutput;
        //deserializeString(split, *downStream);
        pipedInput = deserializeInt(*downStream);
        pipedOutput = deserializeInt(*downStream);
        handler->runCleanup(pipedInput, pipedOutput);
        break;
      }
        
      case GET_MSG_COUNT: {
        int msgCount = deserializeInt(*downStream);
        handler->setNewResult(msgCount);
        break;
      }
      case GET_MSG: {
        string msg;
        deserializeString(msg,*downStream);
        handler->setNewResult(msg);
        break;
      }
      case GET_PEERNAME: {
        string peername;
        deserializeString(peername,*downStream);
        handler->setNewResult(peername);
        break;
      }
      case GET_ALL_PEERNAME: {
        vector<string> peernames;
        int peernameCount = deserializeInt(*downStream);
        string peername;
        for (int i=0; i<peernameCount; i++)  {
          deserializeString(peername,*downStream);
            peernames.push_back(peername);
        }
        handler->setNewResult(peernames);
        break;
      }
      
    

    /*
      case RUN_REDUCE: {
        int32_t reduce;
        int32_t piped;
        reduce = deserializeInt(*downStream);
        piped = deserializeInt(*downStream);
        handler->runReduce(reduce, piped);
        break;
      }
      case REDUCE_KEY: {
        deserializeString(key, *downStream);
        handler->reduceKey(key);
        break;
      }
      case REDUCE_VALUE: {
        deserializeString(value, *downStream);
        handler->reduceValue(value);
        break;
      }
      */
              
              
      case CLOSE:
        handler->close();
        break;
      case ABORT:
        handler->abort();
        break;
      default:
        HADOOP_ASSERT(false, "Unknown binary command " + toString(cmd));
      }
    }
      
    virtual ~BinaryProtocol() {
      delete downStream;
      delete uplink;
    }
  };

  /********************************************/
  /************** BSPContextImpl **************/  
  /********************************************/
  class BSPContextImpl: public BSPContext, public DownwardProtocol {
  private:
    bool done;
    BSPJob* job;
    string key;
    const string* newKey;
    const string* value;
    bool hasTask;
    bool isNewKey;
    bool isNewValue;
    string* inputKeyClass;
    string* inputValueClass;
    
    //string status;
    //float progressFloat;
    //uint64_t lastProgress;
    //bool statusSet;
      
    Protocol* protocol;
    UpwardProtocol *uplink;
    
    string* inputSplit;
    
    RecordReader* reader;
    RecordWriter* writer;
      
    BSP* bsp;
    //Mapper* mapper;
    
    const Factory* factory;
    pthread_mutex_t mutexDone;
    std::vector<int> registeredCounterIds;
      
    int resultInt;
    bool isNewResultInt;  
    const string* resultString;
    bool isNewResultString;   
    vector<string> resultVector;
    bool isNewResultVector; 

  public:

    BSPContextImpl(const Factory& _factory) {
      //statusSet = false;
      done = false;
      newKey = NULL;
      factory = &_factory;
      job = NULL;
        
      inputKeyClass = NULL;
      inputValueClass = NULL;
      
      inputSplit = NULL;
      
      bsp = NULL;
      reader = NULL;
      writer = NULL;
      //partitioner = NULL;
      protocol = NULL;
      isNewKey = false;
      isNewValue = false;
      //lastProgress = 0;
      //progressFloat = 0.0f;
      hasTask = false;
      pthread_mutex_init(&mutexDone, NULL);
        
      isNewResultInt = false;
      isNewResultString = false,
      isNewResultVector = false;
    }

  
    /********************************************/
    /*********** DownwardProtocol IMPL **********/  
    /********************************************/
    virtual void start(int protocol) {
      if (protocol != 0) {
        throw Error("Protocol version " + toString(protocol) + 
                    " not supported");
      }
    }

    virtual void setBSPJob(vector<string> values) {
      int len = values.size();
      BSPJobImpl* result = new BSPJobImpl();
      HADOOP_ASSERT(len % 2 == 0, "Odd length of job conf values");
      for(int i=0; i < len; i += 2) {
        result->set(values[i], values[i+1]);
      }
      job = result;
    }

    virtual void setInputTypes(string keyType, string valueType) {
      inputKeyClass = new string(keyType);
      inputValueClass = new string(valueType);
    }
      
    virtual void setKeyValue(const string& _key, const string& _value) {
      newKey = &_key;
      value = &_value;
      isNewKey = true;
    }
     
    /* private Method */
    void setupReaderWriter(bool pipedInput, bool pipedOutput) {
        
      if (reader==NULL) {
        reader = factory->createRecordReader(*this);
        HADOOP_ASSERT((reader == NULL) == pipedInput,
                      pipedInput ? "RecordReader defined when not needed.":
                      "RecordReader not defined");
        if (reader != NULL) {
            value = new string();
        }
      }  
        
      if (writer==NULL) {
        writer = factory->createRecordWriter(*this);
        HADOOP_ASSERT((writer == NULL) == pipedOutput,
                      pipedOutput ? "RecordWriter defined when not needed.":
                      "RecordWriter not defined");
      }
    }
      
    virtual void runSetup(bool pipedInput, bool pipedOutput) {
      setupReaderWriter(pipedInput,pipedOutput);
      
      if (bsp == NULL)  
        bsp = factory->createBSP(*this);
        
      if (bsp != NULL) {
        bsp->setup(*this);
      }
    }
      
    virtual void runBsp(bool pipedInput, bool pipedOutput) {
      setupReaderWriter(pipedInput,pipedOutput);
    
      if (bsp == NULL)  
          bsp = factory->createBSP(*this);
        
      hasTask = true;
    }
      
    virtual void runCleanup(bool pipedInput, bool pipedOutput) {
      setupReaderWriter(pipedInput,pipedOutput);
        
      if (bsp != NULL) {
        bsp->cleanup(*this);
      }
    }
     
    virtual void setNewResult(int value) {
      resultInt = value;
      isNewResultInt = true;  
    }

    virtual void setNewResult(const string& value) {
      resultString = &value;
      isNewResultString = true;   
    }

    virtual void setNewResult(vector<string> value) {
      resultVector = value;
      isNewResultVector = true;    
    }

    virtual void close() {
      pthread_mutex_lock(&mutexDone);
      done = true;
      pthread_mutex_unlock(&mutexDone);
    }
      
    virtual void abort() {
      throw Error("Aborted by driver");
    }

    /********************************************/
    /************** TaskContext IMPL *************/  
    /********************************************/
    
    /**
     * Get the BSPJob for the current task.
     */
    virtual BSPJob* getBSPJob() {
      return job;
    }

    /**
     * Get the current key. 
     * @return the current key or NULL if called before the first map or reduce
     */
    virtual const string& getInputKey() {
      return key;
    }

    /**
     * Get the current value. 
     * @return the current value or NULL if called before the first map or 
     *    reduce
     */
    virtual const string& getInputValue() {
      return *value;
    }
      
    /**
     * Register a counter with the given group and name.
     */
    /*
    virtual Counter* getCounter(const std::string& group, 
                                  const std::string& name) {
        int id = registeredCounterIds.size();
        registeredCounterIds.push_back(id);
        uplink->registerCounter(id, group, name);
        return new Counter(id);
    }*/
      
    /**
     * Increment the value of the counter with the given amount.
     */
    virtual void incrementCounter(const string& group, const string& name, uint64_t amount)  {
        uplink->incrementCounter(group, name, amount); 
    }
      
    /********************************************/
    /************** BSPContext IMPL *************/  
    /********************************************/
      
    /**
     * Access the InputSplit of the mapper.
     */
    virtual const std::string& getInputSplit() {
      return *inputSplit;
    }
      
    /**
     * Get the name of the key class of the input to this task.
     */
    virtual const string& getInputKeyClass() {
      return *inputKeyClass;
    }

    /**
     * Get the name of the value class of the input to this task.
     */
    virtual const string& getInputValueClass() {
      return *inputValueClass;
    }

    /**
     * Send a data with a tag to another BSPSlave corresponding to hostname.
     * Messages sent by this method are not guaranteed to be received in a sent
     * order.
     */
    virtual void sendMessage(const string& peerName, const string& msg) {
        uplink->sendMessage(peerName,msg);
    }
      
    /**
     * @return A message from the peer's received messages queue (a FIFO).
     */
    virtual const string& getCurrentMessage() {
      uplink->getMessage();
      
      while (!isNewResultString)
          protocol->nextEvent();
        
      isNewResultString = false;
      return *resultString;
    }

    /**
     * @return The number of messages in the peer's received messages queue.
     */
    virtual int getNumCurrentMessages() {
      uplink->getMessageCount();
        
      while (!isNewResultInt)
        protocol->nextEvent();
      
      isNewResultInt = false;
      return resultInt;
    }
      
    /**
     * Barrier Synchronization.
     * 
     * Sends all the messages in the outgoing message queues to the corresponding
     * remote peers.
     */
    virtual void sync() {
      uplink->sync();
    }
      
    /**
     * @return the count of current super-step
     */
    virtual long getSuperstepCount() {
      return 0;
    }
    
    /**
     * @return the name of this peer in the format "hostname:port".
     */ 
    virtual const string& getPeerName() {
      uplink->getPeerName(-1);
    
      while (!isNewResultString)
        protocol->nextEvent();
    
      isNewResultString = false;
      return *resultString;
    }
    
    /**
     * @return the name of n-th peer from sorted array by name.
     */
    virtual const string& getPeerName(int index) {
      uplink->getPeerName(index);
        
      while (!isNewResultString)
        protocol->nextEvent();
        
      isNewResultString = false;
      return *resultString;
    }
      
    /**
     * @return the index of this peer from sorted array by name.
     */
    virtual int getPeerIndex() {
      return 0;
    }
    
    /**
     * @return the names of all the peers executing tasks from the same job
     *         (including this peer).
     */
    virtual vector<string> getAllPeerNames() {
      uplink->getAllPeerNames();
        
      while (!isNewResultVector)
        protocol->nextEvent();
        
      isNewResultVector = false;
      return resultVector;
    }
    
    /**
     * @return the number of peers
     */
    virtual int getNumPeers() {
      return 0;         
    }
    
    /**
     * Clears all queues entries.
     */
    virtual void clear() {
          
    }

    /**
     * Writes a key/value pair to the output collector
     */
    virtual void write(const string& key, const string& value) {
        if (writer != NULL) {
            writer->emit(key, value);
        } else {
            uplink->writeKeyValue(key, value);
        }
    }
    
    /**
     * Deserializes the next input key value into the given objects;
     */
    //virtual bool readNext(const string& key, const string& value) {
    //  return protocol->readNext(key,value);
    //}
       
    /**
     * Closes the input and opens it right away, so that the file pointer is at
     * the beginning again.
     */
    virtual void reopenInput() {
      uplink->reopenInput();
    }
      
      
    /********************************************/
    /*************** Other STUFF  ***************/  
    /********************************************/
      
    void setProtocol(Protocol* _protocol, UpwardProtocol* _uplink) {
        protocol = _protocol;
        uplink = _uplink;
    }
   
    virtual bool isDone() {
        pthread_mutex_lock(&mutexDone);
        bool doneCopy = done;
        pthread_mutex_unlock(&mutexDone);
        return doneCopy;
    }
      
    /**
     * Advance to the next value.
     */
    virtual bool nextValue() {
        if (isNewKey || done) {
            return false;
        }
        isNewValue = false;
        //progress();
        protocol->nextEvent();
        return isNewValue;
    } 
      
    void waitForTask() {
        while (!done && !hasTask) {
            protocol->nextEvent();
        }
    }
      
    bool nextKey() {
        if (reader == NULL) {
            while (!isNewKey) {
                nextValue();
                if (done) {
                    return false;
                }
            }
            key = *newKey;
        } else {
            if (!reader->next(key, const_cast<string&>(*value))) {
                pthread_mutex_lock(&mutexDone);
                done = true;
                pthread_mutex_unlock(&mutexDone);
                return false;
            }
            //progressFloat = reader->getProgress();
        }
        isNewKey = false;
          
        if (bsp != NULL) {
            bsp->bsp(*this);
        }
        return true;
    }
   
    void closeAll() {
      if (reader) {
        reader->close();
      }
      
      if (bsp) {
        bsp->close();
      }
     
      if (writer) {
        writer->close();
      }
    }
      
    virtual ~BSPContextImpl() {
      delete job;
      delete inputKeyClass;
      delete inputValueClass;
      delete inputSplit;
      if (reader) {
        delete value;
      }
      delete reader;
      delete bsp;
      delete writer;
      pthread_mutex_destroy(&mutexDone);
    }
  };

  /**
   * Ping the parent every 5 seconds to know if it is alive 
   */
  void* ping(void* ptr) {
    BSPContextImpl* context = (BSPContextImpl*) ptr;
    char* portStr = getenv("hama.pipes.command.port");
    int MAX_RETRIES = 3;
    int remaining_retries = MAX_RETRIES;
    while (!context->isDone()) {
      try{
        sleep(5);
        int sock = -1;
        if (portStr) {
          sock = socket(PF_INET, SOCK_STREAM, 0);
          HADOOP_ASSERT(sock != - 1,
                        string("problem creating socket: ") + strerror(errno));
          sockaddr_in addr;
          addr.sin_family = AF_INET;
          addr.sin_port = htons(toInt(portStr));
          addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
          HADOOP_ASSERT(connect(sock, (sockaddr*) &addr, sizeof(addr)) == 0,
                        string("problem connecting command socket: ") +
                        strerror(errno));

        }
        if (sock != -1) {
          int result = shutdown(sock, SHUT_RDWR);
          HADOOP_ASSERT(result == 0, "problem shutting socket");
          result = close(sock);
          HADOOP_ASSERT(result == 0, "problem closing socket");
        }
        remaining_retries = MAX_RETRIES;
      } catch (Error& err) {
        if (!context->isDone()) {
          fprintf(stderr, "Hama Pipes Exception: in ping %s\n", 
                err.getMessage().c_str());
          remaining_retries -= 1;
          if (remaining_retries == 0) {
            exit(1);
          }
        } else {
          return NULL;
        }
      }
    }
    return NULL;
  }

  /**
   * Run the assigned task in the framework.
   * The user's main function should set the various functions using the 
   * set* functions above and then call this.
   * @return true, if the task succeeded.
   */
  bool runTask(const Factory& factory) {
    try {
      BSPContextImpl* context = new BSPContextImpl(factory);
      Protocol* connection;
        
      char* portStr = getenv("hama.pipes.command.port");
      int sock = -1;
      FILE* stream = NULL;
      FILE* outStream = NULL;
      char *bufin = NULL;
      char *bufout = NULL;
      if (portStr) {
        sock = socket(PF_INET, SOCK_STREAM, 0);
        HADOOP_ASSERT(sock != - 1,
                      string("problem creating socket: ") + strerror(errno));
        sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(toInt(portStr));
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        HADOOP_ASSERT(connect(sock, (sockaddr*) &addr, sizeof(addr)) == 0,
                      string("problem connecting command socket: ") +
                      strerror(errno));

        stream = fdopen(sock, "r");
        outStream = fdopen(sock, "w");

        // increase buffer size
        int bufsize = 128*1024;
        int setbuf;
        bufin = new char[bufsize];
        bufout = new char[bufsize];
        setbuf = setvbuf(stream, bufin, _IOFBF, bufsize);
        HADOOP_ASSERT(setbuf == 0, string("problem with setvbuf for inStream: ")
                                     + strerror(errno));
        setbuf = setvbuf(outStream, bufout, _IOFBF, bufsize);
        HADOOP_ASSERT(setbuf == 0, string("problem with setvbuf for outStream: ")
                                     + strerror(errno));
          
        connection = new BinaryProtocol(stream, context, outStream);
          
      } else if (getenv("hama.pipes.command.file")) {
        char* filename = getenv("hama.pipes.command.file");
        string outFilename = filename;
        outFilename += ".out";
        stream = fopen(filename, "r");
        outStream = fopen(outFilename.c_str(), "w");
        connection = new BinaryProtocol(stream, context, outStream);
      } else {
        //connection = new TextProtocol(stdin, context, stdout);
      }
        
      context->setProtocol(connection, connection->getUplink());
        
      pthread_t pingThread;
      pthread_create(&pingThread, NULL, ping, (void*)(context));
      context->waitForTask();
        
      while (!context->isDone()) {
        context->nextKey();
      }
        
      context->closeAll();
      connection->getUplink()->done();
      
      pthread_join(pingThread,NULL);
      
      delete context;
      delete connection;
      if (stream != NULL) {
        fflush(stream);
      }
      if (outStream != NULL) {
        fflush(outStream);
      }
      fflush(stdout);
      if (sock != -1) {
        int result = shutdown(sock, SHUT_RDWR);
        HADOOP_ASSERT(result == 0, "problem shutting socket");
        result = close(sock);
        HADOOP_ASSERT(result == 0, "problem closing socket");
      }
      if (stream != NULL) {
        //fclose(stream);
      }
      if (outStream != NULL) {
        //fclose(outStream);
      } 
      delete bufin;
      delete bufout;
      return true;
    } catch (Error& err) {
      fprintf(stderr, "Hama Pipes Exception: %s\n", 
              err.getMessage().c_str());
      return false;
    }
  }
}

