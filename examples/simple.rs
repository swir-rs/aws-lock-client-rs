#[macro_use]
extern crate log;
extern crate aws_lock_client;

use rusoto_dynamodb;
use rusoto_core;
use rusoto_core::Region;
use rusoto_dynamodb::{DynamoDb, DynamoDbClient};
use std::time::Duration;

use std::env;
use aws_lock_client::AwsLockClient;
use aws_lock_client::AwsLockClientDynamoDb;

#[tokio::main]
async fn main() {
    env_logger::builder().format_timestamp_nanos().init();
    if let Err(_) = env::var("AWS_ACCESS_KEY_ID"){
	error!("AWS_ACCESS_KEY_ID not set");
	return 
    }
    if let Err(_) = env::var("AWS_SECRET_ACCESS_KEY"){
	error!("AWS_SECRET_ACCESS_KEY not set");
	return 
    }


    let client = DynamoDbClient::new(Region::EuWest1);

    let dti = rusoto_dynamodb::DescribeTableInput{
	table_name:"lock_table".to_string()
    };
    
    let dto = client.describe_table(dti).await;
    if let Err(_) = dto{
	let cti = rusoto_dynamodb::CreateTableInput{
	    table_name: "lock_table".to_string(),
	    attribute_definitions:vec![rusoto_dynamodb::AttributeDefinition{attribute_name:"partition_key".to_string(), attribute_type:"S".to_string()}],
	    key_schema:vec![rusoto_dynamodb::KeySchemaElement{attribute_name: "partition_key".to_string(),key_type: "HASH".to_string()}],
	    billing_mode: Some("PAY_PER_REQUEST".to_string()),
	    .. Default::default() 
	};


	let cto = client.create_table(cti).await;
	match cto {
	    Err(_)=> {
		error!("Unable to create table");
		error!("{:?}",cto);
		return;
	    }
	    Ok(_)=> {}
	}
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;
    

    let stream_name = "some_stream_name_name";
    let shard_id = "shardId-00001";
    let key = format!("{}-{}",stream_name, shard_id);
    let aws_lock_client = AwsLockClientDynamoDb::new(Region::EuWest1,"lock_table".to_string());    

    for _ in 0..1 {
	info!("Creating lock with no data");
	let maybe_lock = aws_lock_client.try_acquire_lock(key.clone(),Duration::from_millis(1000)).await;	
	info!("Lock ? {:?}",maybe_lock);
	match maybe_lock{
	    Ok(mut lock)=>{
		let mut data = if let Ok(data) = lock.lock_data.unwrap().parse::<i32>(){
		    info!("Found an existing lock");
		    info!("Old data = {}",data);
		    data
		}else{
		    info!("Updating lock with some data");
		    lock.lock_data = Some(100.to_string());
		    let res = aws_lock_client.update_lock(key.clone(), lock.clone()).await;
		    info!{"{:?}",res};
		    100
		};
		
		data += 100;
		lock.lock_data = Some(data.to_string());
		info!("Releasing lock and updating data");
		let res = aws_lock_client.release_lock(key.clone(), lock).await;
		info!{"{:?}",res};
		
	    },
	    Err(_)=> {}
	}
    }
}
