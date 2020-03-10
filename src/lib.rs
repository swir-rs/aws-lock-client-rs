#[macro_use]
extern crate log;

use rusoto_dynamodb;
use rusoto_dynamodb::{DynamoDb, DynamoDbClient};
use std::collections::HashMap;
use async_trait::async_trait;
use hostname;
use uuid;
use std::time::{Duration};
use currenttimemillis::current_time_milliseconds;

const LOCK_ADDITIONAL_HOLD_TIME: u32 = 500;


#[async_trait]
pub trait AwsLockClient {
    async fn acquire_lock(&self)->Result<(),()>;
    async fn try_acquire_lock(&self,key:String,  duration: Duration, data:String )->Result<LockDescription,()>;
    async fn release_lock(&self,key:String, lock:LockDescription)->Result<LockDescription,LockDescription>;
    async fn expires_in(&self,key:String)->Option<u128>;
}
trait Empty {
    fn empty()->Self;
}

#[derive(Debug,Clone)]
pub struct LockDescription{
    lock_id: Option<String>,
    lock_owner:Option<String>,
    lock_created:Option<u128>,
    lock_last_updated:Option<u128>,
    lock_lease_duration:Option<u128>,
    lock_released:Option<bool>,
    pub lock_data:Option<String>
}

impl LockDescription{
    fn check_lock_expired(&self)->bool{
	let created = self.lock_created.unwrap_or(0);
	let lease_duration = self.lock_lease_duration.unwrap_or(0);
	let released = self.lock_released.unwrap_or(false);
	let lock_expired_time= created + lease_duration + u128::from(LOCK_ADDITIONAL_HOLD_TIME);
	debug!("{} {}",lock_expired_time,current_time_milliseconds());
	if lock_expired_time < current_time_milliseconds(){
	    true
	}else{
	    if released{
		true
	    }else{
		false
	    }
	}
    }

    fn expires_in(&self)->Option<u128>{
	let created = self.lock_created.unwrap_or(0);
	let lease_duration = self.lock_lease_duration.unwrap_or(0);
	let lock_expired_time= created + lease_duration + u128::from(LOCK_ADDITIONAL_HOLD_TIME);
	lock_expired_time.checked_sub(current_time_milliseconds())
    }
    
}

impl Empty for LockDescription{
    fn empty()->Self{
	LockDescription{
	    lock_id: None,
	    lock_owner:None,
	    lock_created: None,
	    lock_last_updated:None,
	    lock_lease_duration:None,
	    lock_released:None,
	    lock_data:None
	}	
    }
}
impl Default for LockDescription{
    fn default()->Self{
	let hostname = hostname::get();
	let hostname = hostname.unwrap_or_default();
	let hostname = hostname.to_string_lossy();
	let lock_owner = format!("{}-{}",hostname,uuid::Uuid::new_v4());
	
	LockDescription{
	    lock_id: Some(uuid::Uuid::new_v4().to_string()),
	    lock_owner:Some(lock_owner),
	    lock_created:Some(current_time_milliseconds()),
	    lock_last_updated:Some(current_time_milliseconds()),
	    lock_lease_duration:Some(5000),
	    lock_released:Some(false),
	    lock_data:None
	}	
    }
}

fn convert_string_to_number(maybe_number:Option<String>)->Option<u128>{
    match maybe_number{
	Some(number)=>{
	    u128::from_str_radix(&number,10).ok()
	},
	None=>None
    }
}

impl From<HashMap<String, rusoto_dynamodb::AttributeValue>> for LockDescription{    
    fn from(item: HashMap<String, rusoto_dynamodb::AttributeValue>) -> Self{
	LockDescription{
	    lock_id: item.get(&"lock_id".to_string()).map(|atrribute_value| atrribute_value.s.to_owned()).flatten(),
	    lock_owner: item.get(&"lock_owner".to_string()).map(|atrribute_value| atrribute_value.s.to_owned()).flatten(),
	    lock_created: item.get(&"lock_created".to_string()).map(|atrribute_value| convert_string_to_number(atrribute_value.n.to_owned())).flatten(),
	    lock_last_updated: item.get(&"lock_last_updated".to_string()).map(|atrribute_value| convert_string_to_number(atrribute_value.n.to_owned())).flatten(),
	    lock_lease_duration: item.get(&"lock_lease_duration".to_string()).map(|atrribute_value| convert_string_to_number(atrribute_value.n.to_owned())).flatten(),
	    lock_released: item.get(&"lock_released".to_string()).map(|atrribute_value| atrribute_value.bool.to_owned()).flatten(),
	    lock_data: item.get(&"lock_data".to_string()).map(|atrribute_value| atrribute_value.s.to_owned()).flatten()
	}
	
    }
}

#[derive(Clone)]
pub struct AwsLockClientDynamoDb{
    client: DynamoDbClient,
    table_name: String
}


impl AwsLockClientDynamoDb {   
    pub fn new(region: rusoto_core::Region,  table_name: String )->Self{
	let client = DynamoDbClient::new(region);

	AwsLockClientDynamoDb{
	    client, table_name
	}    
    }

    fn get_partition_key(&self,key:String)->HashMap<String,rusoto_dynamodb::AttributeValue> {
	let partition_key_attr = rusoto_dynamodb::AttributeValue{
 	    s:Some(key),
 	    .. Default::default()			
 	};
	let mut key = HashMap::new();
	key.insert("partition_key".to_string(),partition_key_attr);
	key
    }
    
    async fn get_lock(&self,key:HashMap<String,rusoto_dynamodb::AttributeValue>)->Option<LockDescription>{
	let client = &self.client;
	let table_name = &self.table_name;
			
	let get_item_input = rusoto_dynamodb::GetItemInput{
	    table_name:table_name.clone(),
	    key: key,
	    .. Default::default()			    			    			    			    
	};

	let get_item_output = client.get_item(get_item_input).await;
	info!("get_lock => {:?}",get_item_output);	
	match get_item_output{
	    Ok(get_item_output)=>{	
		match get_item_output.item{
		    Some(item)=>{
			Some(LockDescription::from(item))			     
		    },
		    None=>{
			None
		    }
		}
	    },
	    Err(_)=>None
	}
    }


    
    
    async fn create_or_update_lock(&self, key:HashMap<String,rusoto_dynamodb::AttributeValue>,lock: LockDescription,is_new:bool, old_lock:LockDescription,)->Result<(),()>{
	let client = &self.client;
	let table_name = &self.table_name;

	
	let lock_id_attr = rusoto_dynamodb::AttributeValue{
	    s:lock.lock_id.clone(),
	    .. Default::default()			
	};

	let old_lock_id_attr = rusoto_dynamodb::AttributeValue{
	    s:old_lock.lock_id.clone(),
	    .. Default::default()			
	};
	
	let lock_owner_attr = rusoto_dynamodb::AttributeValue{
	    s:lock.lock_owner.clone(),
	    .. Default::default()			
	};

	let old_lock_owner_attr = rusoto_dynamodb::AttributeValue{
	    s:old_lock.lock_owner.clone(),
	    .. Default::default()			
	};

	
	let lock_created_attr = rusoto_dynamodb::AttributeValue{
	    n:lock.lock_created.map(|o| o.to_string()),
	    .. Default::default()			
	};


	let lock_lease_duration_attr = rusoto_dynamodb::AttributeValue{
	    n:lock.lock_lease_duration.map(|o| o.to_string()),
	    .. Default::default()			
	};

	let lock_released_attr = rusoto_dynamodb::AttributeValue{
	    bool:lock.lock_released,
	    .. Default::default()			
	};

	let lock_last_updated_attr = rusoto_dynamodb::AttributeValue{
	    n:lock.lock_last_updated.map(|o| o.to_string()),
	    .. Default::default()			
	};

	let old_lock_last_updated_attr = rusoto_dynamodb::AttributeValue{
	    n:old_lock.lock_last_updated.map(|o| o.to_string()),
	    .. Default::default()			
	};

	let data_attr = rusoto_dynamodb::AttributeValue{
	    s:lock.lock_data.map(|o| o.to_string()),
	    .. Default::default()			
	};

	let mut expression_attribute_values= HashMap::new();
	
	expression_attribute_values.insert(":lock_id".to_string(),lock_id_attr);
	expression_attribute_values.insert(":lock_owner".to_string(),lock_owner_attr);
	expression_attribute_values.insert(":lock_released".to_string(),lock_released_attr);
	expression_attribute_values.insert(":lock_created".to_string(),lock_created_attr);
	expression_attribute_values.insert(":lock_lease_duration".to_string(),lock_lease_duration_attr);
	expression_attribute_values.insert(":lock_last_updated".to_string(),lock_last_updated_attr);
	expression_attribute_values.insert(":lock_data".to_string(),data_attr);
	
	
	let condition_expression;
	if is_new{
	    condition_expression = String::from("attribute_not_exists(lock_id)");	    
	}else{
	    condition_expression = String::from("lock_id=:old_lock_id AND lock_owner=:old_lock_owner AND lock_last_updated=:old_lock_last_updated");
	    expression_attribute_values.insert(":old_lock_owner".to_string(),old_lock_owner_attr);
	    expression_attribute_values.insert(":old_lock_id".to_string(),old_lock_id_attr);
	    expression_attribute_values.insert(":old_lock_last_updated".to_string(),old_lock_last_updated_attr);
	};
	
	let update_item_input = rusoto_dynamodb::UpdateItemInput{
	    table_name:table_name.clone(),
	    key: key.clone(),
	    update_expression:Some(String::from("set lock_id=:lock_id, lock_owner=:lock_owner, lock_created=:lock_created, lock_lease_duration=:lock_lease_duration, lock_last_updated=:lock_last_updated,lock_released=:lock_released, lock_data=:lock_data")),
	    condition_expression:Some(condition_expression),
	    expression_attribute_values:Some(expression_attribute_values),
	    return_values: Some(String::from("ALL_OLD")),
	    .. Default::default()			    			    			    			    
	};	
	
	let update_item_output = client.update_item(update_item_input).await;
	info!("create_or_update_lock {} => {:?}",is_new,update_item_output);
	match update_item_output{
	    Ok(_) =>{
		
		Ok(())
	    },	    
	    Err(_) => {
		Err(())
	    }
	}
    }


    async fn update_lock(&self, key:HashMap<String,rusoto_dynamodb::AttributeValue>, lock: LockDescription,old_lock:LockDescription)->Result<(),()>{
	self.create_or_update_lock(key, lock,false,old_lock).await
    }
    
    async fn put_new_lock(&self,key:HashMap<String,rusoto_dynamodb::AttributeValue>, lock: LockDescription)->Result<(),()>{
	self.create_or_update_lock(key, lock,true,LockDescription::empty()).await
    }
    
}

#[async_trait]
impl AwsLockClient for AwsLockClientDynamoDb{
    async fn try_acquire_lock(&self,key:String, duration: Duration, data: String)->Result<LockDescription,()>{
	let key = self.get_partition_key(key);	
	let lock = self.get_lock(key.clone()).await;
	let mut new_lock = LockDescription::default();
	new_lock.lock_lease_duration = Some(duration.as_millis());
	new_lock.lock_data = Some(data);

	    
	match lock{	    
	    Some(lock)=>{
		if lock.lock_data.is_some(){
		    new_lock.lock_data= lock.lock_data.clone();
		}
		if lock.check_lock_expired(){
		    match self.update_lock(key, new_lock.clone(),lock).await{
			Ok(_)=> {
			    debug!("try_acquire_lock successfully locked {:?}",new_lock);
			    Ok(new_lock)
			},
			Err(_)=> Err(())
		    }		    
		}else{
		    debug!("Lock expires in {:?}",lock.expires_in());
		    Err(())
		}
	    },
	    None=>{
		let new_lock = LockDescription::default();		    
		match self.put_new_lock(key, new_lock.clone()).await{
		    Ok(_)=> {
			debug!("try_acquire_lock successfully locked {:?}",new_lock);
			Ok(new_lock)
		    },
		    Err(_)=> Err(())
		}		    
	    }
	}

    }
    async fn acquire_lock(&self)->Result<(),()>{
	Ok(())
    }
    async fn release_lock(&self,key: String, lock:LockDescription)->Result<LockDescription,LockDescription>{
	let key = self.get_partition_key(key);	
	let mut current_lock = lock.clone();
	current_lock.lock_released = Some(true);
	current_lock.lock_last_updated=Some(current_time_milliseconds());
	match self.update_lock(key, current_lock.clone(), lock.clone()).await{
	    Ok(_)=> {
		info!("release_lock => Lock released {:?}",current_lock);		
		Ok(current_lock)		    
	    },
	    Err(_)=> {
		info!("release_lock => Unable to release lock {:?}",current_lock);
		Err(current_lock)
	    }
	}		    	  	
    }

    async fn expires_in(&self, key:String)->Option<u128>{
	let key = self.get_partition_key(key);	
	let lock = self.get_lock(key).await;
	if let Some(lock)= lock{
	    lock.expires_in()
	}else{
	    None
	}
    }
    
}
