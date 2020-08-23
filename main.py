#python
#Libraries
import os, json, requests, base64, shelve, asyncio, logging, csv, pickle, re
from functools import lru_cache
from time import sleep


path = os.path.dirname(os.path.realpath(__name__))
db_path = path + "/db/"
creds = json.load(open(path + "/auth/" + "creds.json", 'r'))
get_conf = json.load(open(path + "/conf/" + "get.json", 'r'))
post_conf = json.load(open(path + "/conf/" + "post.json", 'r'))

#Logging
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', handlers=[logging.FileHandler(path + '/main.log', 'a', 'utf-8')], level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)



class db:
    def __init__(self):
        self.cache = {}
        self.items_limit = 50

    def check_cache(self, get_id: str):
        if len(self.cache[get_id]) > self.items_limit:
            self.write_rows_to_csv(get_id)

    def write_rows_to_csv(self, get_id: str):
        with open(db_path + get_id + ".csv", 'a') as writeFile:
                for row in self.cache[get_id]:
                    json.dump(row, writeFile)
                    writeFile.write(os.linesep)
                
        self.cache[get_id] = []


    def putItem(self, get_id: str, item: dict):
        if get_id not in self.cache.keys():
            self.cache[get_id] = []

        self.cache[get_id].append(item)
        self.check_cache(get_id)

    def putItems(self, get_id: str, items: list):
        assert len([x for x in items if type(x) != dict]) == 0, "Found non-dict types within a list"

        for item in items:
            self.putItem(get_id, item)

    def loadItem(self, get_id: str, **kwargs):

        if get_id in self.cache.keys():
            if self.cache[get_id] != []:
                self.write_rows_to_csv(get_id)

        if os.path.isfile(db_path + get_id + ".csv") == True:
            with open(db_path + get_id + ".csv", 'r') as openfile:
                self.cache[get_id] = []
                if "processing_batch" in kwargs.keys():
                    for counter, line in enumerate(openfile):
                        if counter >= kwargs["processing_batch"][0] and counter < kwargs["processing_batch"][1]:
                            if line not in (None, "", "\n"):
                                self.cache[get_id].append(json.loads(line))
                else:
                    for counter, line in enumerate(openfile):
                        if line not in (None, "", "\n"):
                            self.cache[get_id].append(json.loads(line))

        else:
            raise ValueError("File '{}' was not found".format(db_path + get_id + ".csv"))

    def count_total_lines(self, get_id: str) -> int:

        counter = 0

        if os.path.isfile(db_path + get_id + ".csv") == True:
            with open(db_path + get_id + ".csv", 'r') as openfile:
                for line in openfile:
                        counter += 1

            return counter
        else:
            raise ValueError("File '{}' was not found".format(db_path + get_id + ".csv"))



    def flush(self, get_id = None):
        if get_id == None:
            for x in self.cache.keys():
                self.write_rows_to_csv(x)
        else:
            self.write_rows_to_csv(get_id)

class parseFunctions(object):

    parseFunctions = ["change_email_domain_to", "convert_null_to_value", "transfer_objects", "apply_regex"]
    

    @classmethod
    def run(cls, original_value, override_schema_value: dict):
        key = [x for x in override_schema_value.keys()][0]
        value = [x for x in override_schema_value.values()][0]

      
        result = getattr(cls, key)(original_value, value)
        return result

    @classmethod
    def apply_regex(cls, row: dict, function_params: dict) -> dict:
        
        def extract_regex():
            if "description" in row.keys():
                regex_result = re.search(function_params['regex'], row['description'])

            if regex_result == None:
                if "comments" in row.keys():
                    for com in row['comments']:
                        if "body" in com.keys():
                            regex_result = re.search(function_params['regex'], com['body'])                            
                            if regex_result != None:
                                return regex_result.group()

                    return regex_result     
                else:
                    return regex_result
            else:
                return regex_result.group()

        payload = str(extract_regex())
        if function_params['remove_characters'] and payload != None and payload:
            for repl in function_params['remove_characters']:
                if repl in payload:
                    payload = str(payload).replace(repl,"")
        assert "custom_fields" in row.keys(), "custom_fields was not found in row item"
        row['custom_fields'].append({"id": function_params['output_to_custom_field_id'], "value": payload})
        return row



    @classmethod
    def change_email_domain_to(cls, original_value: str, domain_string: str) -> str:
        if original_value != None and "@" in original_value:
            original_value = original_value.split("@")
            original_value[1] = domain_string if "@" in domain_string else "@" + domain_string
            original_value = "".join(original_value)
            return original_value
        else:
            return original_value

    @classmethod
    def convert_null_to_value(cls, original_value, replace_null_with):
        if original_value == None:
            return replace_null_with
        else:
            return original_value

    @classmethod
    def transfer_objects(cls, row, global_func) -> dict:
        for func in global_func:
            if type(func['field_name']) == dict:
                field_key = [x for x in func['field_name'].keys()][0]
                field_value = [x for x in func['field_name'].values()][0]
                assert field_key in row.keys(), "{} was not found within data row {}".format(field_key, row.items())
                for inner_index, inner_item in enumerate(row[field_key]):
                    assert field_value in inner_item.keys(), "{} was not found within custom_fields inner item. inner_item object {}".format(field_value, inner_item)
                    if str(inner_item[field_value]) == str(func['from_id']):
                        row[field_key][inner_index][field_value] = func['to_id']

                        
            else:
                assert func['field_name'] in row.keys(), "{} was not found in data row".format(func['field_name'])
                if str(row[func['field_name']]) == str(func['from_id']):
                    row[func['field_name']] = func['to_id']

                   

        return row

    @classmethod
    def transfer_to_comments(cls, row: dict, global_func: list) -> dict:

        assert "comments" in row.keys(), "comments field was not found within row items: {}".format(row.items())
        for item in global_func:
            assert item['field_name'] in row.keys(), "{} was not found within row items: {}".format(item, row.items())
            if row[item['field_name']] not in [[], None, ""]:
                fustr = ""
                for fit in row[item['field_name']]:
                    fustr = fustr + " " + str(item['prefix']) +str(fit) + ","
                row["comments"].append({"body": item['field_name'] + ": " + fustr, "public": False})

        return row


    @classmethod
    def clean_objects(cls, row: dict, global_func: list) -> dict:
        for item in global_func:
            assert item["field"] in row.keys(), "Field {} was not found in row keys".format(item["field"])
            if type(item['keep_fields']) == dict:
                
                #filter
                for key, value in item['keep_fields'].items():
                    row[item['field']] = [x for x in row[item['field']] if x[key] in value]

            elif type(item['keep_fields']) == list:
                row[item['field']] = [{k: v for k, v in x.items() if k in item['keep_fields']} for x in row[item['field']]]


            else:
                raise ValueError("keep_fields should be either dictionary to specify fields and filters, or list")

        return row





class zenDeskApiClient:
    
    def __init__ (self, **kwargs):
        assert "base url" in kwargs, "'base url' was not found in supplied arguments"
        self._base_url = kwargs['base url'] if kwargs['base url'][-1] == "/" else kwargs['base url'] + "/"
        self.max_attempts = 3

        auth_types = {'basic': [], 'api token': ['email', 'token'], 'oauth': []}
        assert "authType" in kwargs, "authType was not found in arguments. These are supported authentifications: {}".format(", ".join(auth_types.keys()))
        assert kwargs['authType'] in auth_types, "Supported auth types are (now only api_token supported): " + ", ".join(auth_types)
        for item in auth_types[kwargs['authType']]:
            assert item in kwargs, "{} was not found in given arguments".format(item)

        self.session = requests.Session()
        auth_string = kwargs['email'] + "/token:" + kwargs['token']
        auth_string = base64.b64encode(auth_string.encode("utf-8"))
        print("Basic " + auth_string.decode("UTF-8"))
        self.session.headers['Authorization'] = "Basic " + auth_string.decode("UTF-8")
        self.max_attempts = 3
        self.sleep_time = 60

    #Decorator definition
    def attempts(function):
        def wrapper(self, **kwargs):
            attempt = 0
            while attempt <= self.max_attempts:
                try:
                    attempt += 1
                    func = function(self, **kwargs)
                    return func

                except Exception as e:
                    logger.info("attempt: {}. {}. Sleeping {} seconds...".format(attempt,e, self.sleep_time))
                    sleep(self.sleep_time)

        return wrapper

    @attempts
    def post(self, **kwargs) -> requests.models.Response:
        if self._base_url not in kwargs['url']:
            kwargs['url'] = self._base_url + kwargs['url']

        response = self.session.post(**kwargs)
        return json.loads(response.text)


    @attempts
    def upload_file(self, filename: str, data: bytes) -> int:
        
        try:
            endpoint = self._base_url + "uploads.json" + "?filename={}".format(filename)
            response = self.session.post(endpoint, data = data, headers = {"Content-Type": "application/binary"})

            assert response.status_code == 201, "Upload error. {}".format(response.text)

            response = response.json()
            assert "upload" in response.keys(), "Upload key missing"
            assert "attachment" in response['upload'].keys(), "attachement key missing"
            assert "token" in response['upload'].keys(), "token key missing"
            return response['upload']['token']

        except Exception as e:
            logger.info("Failed uploading file {}".format(filename))
    
    @attempts
    def create_user(self, user_dict: dict) -> dict:

        assert "name" in user_dict.keys() and "email" in user_dict.keys(), "User payload must contain name and email as minumum input"
        endpoint = self._base_url + "users/create_or_update.json"
        headers =  {"Content-Type": "application/json"}
        #logging.debug({"user": user_dict})
        response = self.session.post(url = endpoint, json = {"user": user_dict}, headers=headers)

        assert response.status_code in [200, 201], "User creation failed, {}".format(response.text)
        assert "user" in response.json().keys(), "user was not found in response {}".format(response.text)

        if response.status_code == 200:
            return {**response.json()["user"], **{"action": "updated"}}

        elif response.status_code == 201:
            return {**response.json()["user"], **{"action": "created"}}

        else:
            raise ValueError("Bad response code {}. Response text: {}".format(response.status_code, response.text))
    
    @attempts
    def _check_user(self, user_id: str) -> bool:
        #function not used
        endpoint = self._base_url + "users/{}.json".format(user_id)
        headers =  {"Content-Type": "application/json"}
        response = self.session.get(url = endpoint, headers=headers)
        
        if response.status_code == 200 and "user" in response.json():
            return True
        else:
            return False

    @attempts
    def check_user_by_external_id(self, external_id: str) -> bool:
        endpoint = self._base_url + "search.json?query=external_id:{}".format(external_id)
        headers =  {"Content-Type": "application/json"}
        response = self.session.get(url = endpoint, headers=headers)
        response = response.json()

        assert "results" in response, "Response error"
        
        if len(response["results"]) > 0:
            return False
        else:
            return True 

    @attempts
    def get_user(self, user_id: str) -> dict:
        endpoint = self._base_url + "users/{}.json".format(user_id)
        headers =  {"Content-Type": "application/json"}
        response = self.session.get(url = endpoint, headers=headers)
        assert response.status_code == 200, "Response was not successful. {}. Url: {}".format(response.text, response.url)
        assert "user" in response.json(), "user key was not found in response list of keys"
        
        return response.json()['user']



    @attempts
    def get_file(self, filename_url: str) -> bytes:
        response = self.session.get(filename_url)

        assert response.status_code == 200, "Response Error during get file. {}".format(response.text)
        return response.content

    @attempts
    def get(self, **kwargs):

        def check_if_to_skip(row: dict) -> bool:
            #CHANGE CONDITIONS FOR SAVING DATA
            if row["id"] >= 40000 and row['group_id'] == 25895609:#!!!!!!!!!!!!!!!!!!!!!!!!!
                #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                child_node_tickets_to_be_skipped = {"status": "deleted"}
                for key, value in child_node_tickets_to_be_skipped.items():
                    if key in row.keys() and row[key] == value:
                        return True
            
                return False
            else:
                return True


        #Check for proper return
        def process_response(response: requests.models.Response, **kwargs):
            response = json.loads(response.text)
            assert type(response) == dict, "Dict/json type response expected. Wrong type returned."
            if "response_name" not in kwargs:
                if ("next_page" in response.keys() or\
                    "after_url" in response.keys() or\
                        "end_of_stream" in response.keys()) == False:
                            logger.info("next_page or end_of_stream or after_url was not found in response keys. Reponse: {}".format(response))
                            raise ValueError("next_page or end_of_stream or after_url was not found in response keys.")
                assert kwargs['name'] in response.keys(), "name key was not found in response doctionary. Debug."
            else:
                assert kwargs['response_name'] in response.keys(), "response_name was not found in response keys"

            return response

        params = ['endpoint']

        for par in params:
            assert par in kwargs, "{} was not found in input parameters. Please provide {}.".format(par, par)

        url = None
        loop = True
        while loop == True:
            response = self.session.get((self._base_url + kwargs['endpoint']) if url == None else url)
            response = process_response(response, **kwargs)
            
            #Process All Child Nodes as defined within conf/get.json
            if "child_nodes" in kwargs:
                
                def process_child(child: dict, parent_id: int):

                    return_data = []
                    child_url = None
                    child_loop = True 
                    
                    while child_loop == True:

                        child_response = self.session.get(
                            (self._base_url + child['endpoint'].format(parent_id)) if child_url == None else child_url
                                                        )

                        child_response = process_response(child_response, **child)

                        if "next_page" in child_response.keys() and child_response['next_page'] != None:
                            child_url = child_response['next_page']
                            return_data.extend(child_response[child['name'] if "response_name" not in child else child['response_name'] ])
                        else:
                            child_loop = False
                            return_data.extend(child_response[child['name'] if "response_name" not in child else child['response_name']])
                    
                    return return_data
                

                for child in kwargs['child_nodes']:

                    for index, parent in  enumerate(response[kwargs['name']]):

                        check_skipping = check_if_to_skip(parent)

                        if check_skipping == False:
                            return_data = process_child(child, parent[child['parent_id']])    
                            response[kwargs['name']][index][child['name']] = return_data

                        else:
                            response[kwargs['name']][index][child['name']] = []
                            logging.debug("Skip child node {} for ticket id {}".format(child, parent[child['parent_id']]))


            if "next_page" in response.keys() and response['next_page'] != None:
                url = response['next_page']
                yield response[kwargs['name']]
                
            elif "end_of_stream" in response.keys() and response["end_of_stream"] == False:
                url = response["after_url"]
                yield response[kwargs['name']]
            
            else:
                loop = False
                yield response[kwargs['name']]


    def __del__(self):
        self.session.close()


class mainHandler:
    def __init__(self):
        self.client = {}
        self.db = db()

    def _instantiate_client(self, auth_id: str):
        if auth_id not in self.client.keys():
            self.client[auth_id] = zenDeskApiClient(**creds[auth_id])

    def get_conf_item(self, conf_item: dict):
        logger.info("Processing conf {}...".format(conf_item['name']))
        
        #Instantiate auth key item if not yet done
        self._instantiate_client(conf_item['auth_id'])

        response = self.client[conf_item['auth_id']].get(**conf_item)
        
        for data in response:
            logger.info("Pulled total items {}".format(len(data)))
            self.db.putItems(conf_item['get_id'], data)
            
        
        self.db.flush()
        logger.info("Processing conf {}... Done".format(conf_item['name']))


    def get_all_conf_items(self, conf_items: list):
        for conf in conf_items:
            logger.info("Getting conf name {}...".format(conf['name']))
            self.get_conf_item(conf)
            logger.info("Getting conf name {}... Done".format(conf['name']))


    
    def post_conf_batch(self, conf_item: dict, batch: list):
        logging.info("Loading batch for {}...".format(conf_item['name']))
        assert "batch_size" in conf_item.keys(), "Please specify batch_size within post conf item"
        assert conf_item["batch_size"] >= len(batch), "Batch is over the limit"


        #Reduce keys for dictionary
        def reduce_and_add_prefix(row, key, value):
            if value != None:
                if "add_prefix" in conf_item.keys() and key in conf_item["add_prefix"].keys():
                    return str(conf_item["add_prefix"][key]) + str(row[value])
                else:
                    return row[value]

            else:
                return None

        for index, row in enumerate(batch):
            batch[index] = {k: reduce_and_add_prefix(row, k, v) for k, v in conf_item['schema'].items()}
       

        #add tags in specified
        if "add" in conf_item.keys():
            for index, row in enumerate(batch):
                for item_key, add_item in conf_item['add'].items():
                    assert type(add_item) == list, "Add item {} should be list, but was {}".format(item_key, type(add_item))
                    if item_key in row.keys():
                        assert type(row[item_key]) == list, "Batch column key {} was expected to be a list, but was: {}".format(item_key, type(row[item_key]))
                        batch[index][item_key] = row[item_key] + add_item
                    else:
                        batch[index][item_key] = add_item


        #final_pass_fields
        if "final_pass_fields" in conf_item.keys():
            batch = [{k: v for k, v in x.items() if k in conf_item['final_pass_fields']} for x in batch]

        batch = {conf_item['name']: batch}
        pickle.dump(batch, open(path + "batch.pkl", "wb"))

        self._instantiate_client(conf_item['auth_id'])

        if "test" in conf_item.keys() and conf_item['test'] == True:
            response = {"test": True}
        else:
            response = self.client[conf_item['auth_id']].post(url=conf_item['endpoint'], json = batch, headers = {"Content-Type": "application/json"})

        logging.info("Loading batch for {}... Response: {}. Done".format(conf_item['name'], json.dumps(response)))

        batch = [{**row, **{"job_status": response}} for row in batch[conf_item['name']]]

        return batch


    def post_conf_all_batches(self, conf_item: dict, **kwargs):

        #inner function variables
        self.counter_check_external_id = 0

        def lookup_conf_auth_source_and_destination() -> dict:
            assert "post_id" in conf_item.keys(), "post_id as not found within conf_item, is it post_conf ?"
            auth_id_destination = conf_item['auth_id']
            
            def get_source_auth_id():
                for x in get_conf:
                    if x['get_id'] == conf_item['get_id']:
                        return x['auth_id']
                else:
                    raise ValueError("Could not get source auth_id")

            auth_id_source = get_source_auth_id()

            return {"auth_id_destination": auth_id_destination, "auth_id_source": auth_id_source}

        def upload_attachements(auth_id_source: str, auth_id_destination: str, content_url: str, file_name: str) -> int:
            
            try:
                for id in [auth_id_source, auth_id_destination]:
                    self._instantiate_client(id)

                get_file_data = self.client[auth_id_source].get_file(filename_url=content_url)
                attachement_token = self.client[auth_id_destination].upload_file(filename=file_name, data=get_file_data)
                return attachement_token
            
            except Exception as e:
                logger.info("Failed uploading attachemement. File content_url: {}".format(content_url))
                return None

        def check_external_id(external_id: str) -> bool:
            if "limit_upload_to" in conf_item.keys():
                if self.counter_check_external_id >= conf_item["limit_upload_to"]:
                    return False
            
            self._instantiate_client(conf_item['auth_id'])
            result = self.client[conf_item['auth_id']].check_user_by_external_id(external_id=external_id)
            logger.debug("Check performed for external_id: {}. Outcome: {}".format(external_id, result))
            if result == True:
                self.counter_check_external_id += 1
            return result

        @lru_cache(maxsize=10000)
        def migrate_user(source_user_id: int, source_auth_id: str, destination_auth_id: str, fields_to_post: tuple) -> str:

            try:

                for id in [source_auth_id, destination_auth_id]:
                    self._instantiate_client(id)
                
                #Get User from the system
                response_step1 = self.client[source_auth_id].get_user(user_id = source_user_id)
                logger.debug("Retrieved user {}".format(source_user_id))

                if "parseFunctions" in conf_item["migrate_users"].keys():
                    #This is a code part where need to implement those email overrides
                    for item in conf_item["migrate_users"]["parseFunctions"]:
                        for key, value in item.items():
                            for item_inner_dict in value:
                                response_step1[key] = parseFunctions.run(response_step1[key], item_inner_dict)

                #add tags
                if "add" in conf_item.keys():
                    if "tags" in conf_item['add'].keys():
                        if "tags" in response_step1.keys():
                            response_step1['tags'].extend(conf_item['add']['tags'])
                        else:
                            response_step1['tags'] = conf_item['add']['tags']

                #set external_id to id
                for item_1_k, item_1_v in conf_item['schema'].items():
                    if item_1_k in response_step1.keys() and item_1_v in response_step1.keys():
                        response_step1[item_1_k] = response_step1[item_1_v]

                #add external_id with prefix and id
                if "add_prefix" in conf_item.keys():
                    assert type(conf_item["add_prefix"]) == dict, "add_prefix should be dictionary value"
                    for add_key, add_value in conf_item["add_prefix"].items():
                        response_step1[add_key] = str(add_value) + str(response_step1[add_key])
                        
                combine_tuples = fields_to_post + tuple(["external_id"])
                payload_dict = {**{k: v for k, v in response_step1.items() if k in combine_tuples}, **{"verified": True}}

                #Post User to destination system
                response_step2 = self.client[destination_auth_id].create_user(user_dict = payload_dict)
                if response_step2['action'] == 'created':
                    logger.debug("Created user {}".format(response_step2))
                elif response_step2['action'] == 'updated':
                    logger.debug("Updated user {}".format(response_step2))
                else:
                    logger.debug("??? user {}".format(response_step2))

                #Return id to reassign to tickets
                return response_step2['id']

            except Exception as e:
                logger.info("Failed migrating user. Source user id {}".format(source_user_id))
                return None


        logging.info("Uploading in batches for {}...".format(conf_item['name']))

        if "processing_batch" in kwargs.keys():
            self.db.loadItem(conf_item['get_id'], kwargs['processing_batch'])
        else:
            self.db.loadItem(conf_item['get_id'])



        #created processed key
        if conf_item["get_id"] + "_processed" not in self.db.cache.keys():
            self.db.cache[conf_item["get_id"] + "_processed"] = []

        


        #Filter data
        if "filters" in conf_item.keys():
            for filter_item_key, filter_item_value in conf_item['filters'].items():
                self.db.cache[conf_item['get_id']] = [row for row in self.db.cache[conf_item['get_id']] if str(row[filter_item_key]) == str(filter_item_value)]


        if "skip_tickets_with_matching_external_id" in conf_item.keys() and conf_item["skip_tickets_with_matching_external_id"] == True:
            if "add_prefix" in conf_item.keys() and "external_id" in conf_item['add_prefix'].keys():
                prefix = conf_item['add_prefix']['external_id']
            else:
                prefix = None

            self.db.cache[conf_item['get_id']] = list(filter(lambda x: check_external_id(external_id=str(prefix) + str(x['id']) if prefix != None else x['id']), self.db.cache[conf_item['get_id']]))
        self.counter_check_external_id = 0
       
        if "limit_upload_to" in conf_item.keys():
            assert type(conf_item['limit_upload_to']) == int, "Please set 'limit_upload_to' to integer value"
            self.db.cache[conf_item['get_id']] = self.db.cache[conf_item['get_id']][:conf_item["limit_upload_to"]]



        logger.info("To be Uploaded conf {}".format(conf_item))
        logger.info("Total items to be processed {}".format(len(self.db.cache[conf_item['get_id']])))

        #Upload and sync comment attachements
        if "upload_comment_attachements" in conf_item.keys() and conf_item["upload_comment_attachements"] == True:
            for row_index, row in enumerate(self.db.cache[conf_item['get_id']]):
                for comment_row_index, comment_row in enumerate(row['comments']):
                    for attachment_row_index, attachement_row in enumerate(comment_row['attachments']):
                        auth_source_destination = lookup_conf_auth_source_and_destination()
                        attachement_token = upload_attachements(
                                            auth_source_destination['auth_id_source'],
                                            auth_source_destination['auth_id_destination'],
                                            attachement_row['content_url'],
                                            attachement_row['file_name']
                                            )


                        if "uploads" not in self.db.cache[conf_item['get_id']][row_index]['comments'][comment_row_index]:
                            self.db.cache[conf_item['get_id']][row_index]['comments'][comment_row_index]["uploads"] = [attachement_token]
                        else:
                            self.db.cache[conf_item['get_id']][row_index]['comments'][comment_row_index]["uploads"].append(attachement_token)

                    del self.db.cache[conf_item['get_id']][row_index]['comments'][comment_row_index]['attachments']
        
        
        #Override schema if needed
        if "override_schema" in conf_item.keys():
            for index, row in enumerate(self.db.cache[conf_item['get_id']]):
                for key, value in conf_item["override_schema"].items():
                    if type(value) != dict:
                        self.db.cache[conf_item['get_id']][index][key] = value
                    elif "parseFunctions" in value.keys():
                        if type(value['parseFunctions']) == list:
                            for func_num, func_name in enumerate(value['parseFunctions']):
                                self.db.cache[conf_item['get_id']][index][key] = parseFunctions.run(self.db.cache[conf_item['get_id']][index][key], value["parseFunctions"][func_num])
                        else:
                            self.db.cache[conf_item['get_id']][index][key] = parseFunctions.run(self.db.cache[conf_item['get_id']][index][key], value["parseFunctions"])

                    else:
                        for key2, value2 in value.items():
                            for inner_index, inner_row in enumerate(self.db.cache[conf_item['get_id']][index][key]):
                                self.db.cache[conf_item['get_id']][index][key][inner_index][key2] = value2





        #Migrate users if specified
        if "migrate_users" in conf_item.keys():
            source_auth_id = [x["auth_id"] for x in get_conf if x['get_id'] == conf_item['get_id']][0]
            assert "fields" in conf_item['migrate_users'].keys(), "Please define fields within conf key 'migrate_users'"

            for row_index, row in enumerate(self.db.cache[conf_item['get_id']]):

                for row_item in conf_item['migrate_users']['fields']:

                    if type(row_item) == dict:

                        for row_item_key, row_item_value in row_item.items():

                            #workaround due to bad data - Georg was playing around
                            if 'comments' not in row.keys():
                                self.db.cache[conf_item['get_id']][row_index]['comments'] = []

                            for inner_row_index, inner_row in enumerate(row[row_item_key]):
                                return_id = migrate_user(inner_row[row_item_value], source_auth_id, conf_item["auth_id"],  tuple(conf_item['migrate_users']["pass_data_to_api"]))
                                self.db.cache[conf_item['get_id']][row_index][row_item_key][inner_row_index][row_item_value] = return_id
                    else:
                        return_id = migrate_user(row[row_item], source_auth_id, conf_item["auth_id"], tuple(conf_item['migrate_users']["pass_data_to_api"]))
                        self.db.cache[conf_item['get_id']][row_index][row_item] = return_id
                     

        if "global_parse_functions" in conf_item.keys():
            for row_index, row in enumerate(self.db.cache[conf_item['get_id']]):
                for global_func in conf_item["global_parse_functions"]:
                    function_return = parseFunctions.run(row, global_func)
                    #logger.debug(global_func)
                    #logger.debug(function_return)
                    if function_return:
                        self.db.cache[conf_item['get_id']][row_index] = function_return



        for num in range(0, len(self.db.cache[conf_item['get_id']]), conf_item['batch_size']):
            response = self.post_conf_batch(conf_item, self.db.cache[conf_item['get_id']][num: conf_item['batch_size'] + num] )


            #extend result
            self.db.cache[conf_item["get_id"] + "_processed"].extend(response)

            #flush
            self.db.flush(conf_item["get_id"] + "_processed")

            #Delete cash
            self.db.cache[conf_item["get_id"]] = []
            

        logging.info("Uploading in batches for {}... Done".format(conf_item['name']))

    def post_all_confs(self, conf_items: list):
        for conf_item in conf_items:

            if "processing_batch" in conf_item.keys():
                total_rows = self.db.count_total_lines(conf_item['get_id'])
                for processing_batch_start in range(0, total_rows, conf['processing_batch']):
                    logger.info("Running processing batch from {} to {}".format(processing_batch_start, processing_batch_start + conf['processing_batch']))
                    self.post_conf_all_batches(conf_item, processing_batch = (processing_batch_start, processing_batch_start + conf['processing_batch']))



            else:
                 self.post_conf_all_batches(conf_item)


if __name__ == '__main__':

    #Instantiate mainHandler
    main_instance = mainHandler()
    
    #Pull items using get.json
    #main_instance.get_all_conf_items([get_conf[0]])
    
    #Push all items using post.json
    main_instance.post_all_confs([post_conf[3]])

 
