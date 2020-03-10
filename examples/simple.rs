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



#[tokio::main(core_threads = 8)]
async fn main() {
    env_logger::builder().format_timestamp_nanos().init();

    env::set_var("AWS_ACCESS_KEY_ID", "AKIASMSQEUYH3TQIJPXR");
    env::set_var("AWS_SECRET_ACCESS_KEY", "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");

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
	    .. Default::default() 
	};


	let cto = client.create_table(cti).await;
	match cto {
	    Err(_)=> error!{"Unable to create table"},
	    Ok(_)=> {}
	}
    }

    tokio::time::delay_for(tokio::time::Duration::from_millis(2000)).await;
    

    let stream_name = "some_stream_name_name";
    let shard_id = "shardId-00001";
    let key = format!("{}-{}",stream_name, shard_id);
    let aws_lock_client = AwsLockClientDynamoDb::new(Region::EuWest1,"lock_table".to_string());
    let mut data = 100;

    for _ in 0..2 {
	let maybe_lock = aws_lock_client.try_acquire_lock(key.clone(),Duration::new(0,10000),data.to_string()).await;	
	info!("Lock ? {:?}",maybe_lock);
	match maybe_lock{
	    Ok(mut lock)=>{
		data = lock.lock_data.unwrap().parse::<i32>().unwrap();
		data += 100;
		lock.lock_data = Some(data.to_string());
		aws_lock_client.release_lock(key.clone(), lock);
	    },
	    Err(_)=> {}
	}
    }
}
