var mongoDBClient =  require('mongodb').MongoClient;
var assert = require('assert');

var url = 'mongodb://localhost:27017/users';

var tableName = process.argv[2];

var totalNumberOfDocsToProcess = 0;

var noOfDocsProcessed = 0;

assert.notEqual(null,tableName);

mongoDBClient.connect(url, function(err, db) {

	assert.equal(null, err);
  	
	console.log('STATUS,Adbase ACCOUNT NO,Adbase ACCOUNT DETAILS,SF ACCOUNT DETAILS' );
	
	findAccountsInAdbasedAcctNameMatch(db);      	
	findAccountsInAdbasedAcctBillingAddrMatch(db);
	findAccountsInAdbasedBillingAddrMatch(db);

	findDuplicatesInAdbase(db);
	

});

var searchInSalesforce = function (db,accountName1,accountName2,billingStreet,city,zip,AdbaseNo,cursor,inputCollection)
{
	cursor.toArray(function (err, result) 
	{
		var acctStatus;
		var adbaseAccountNo;
		var adbaseDetails;
		var sfDetails = new Array();		
		
		if (err) 
		{
			console.log(err);
		} 
		else if (result.length) 
		{		
			if(result.length > 1)
			{
				acctStatus = 'MULTIPLE FOUND';
				
				adbaseAccountNo  = AdbaseNo;
				adbaseDetails =  accountName1 +  ',' + accountName2 + ',' + billingStreet + ',' + city + ',' + zip;
				
				var logString = 'MULTIPLE FOUND,"' + AdbaseNo  + '","' + accountName1 +  ',' + accountName2 + ',' + billingStreet + ',' + city + ',' + zip;
				
				for(var j = 0 ; j < result.length; j++)
				{
					logString += '","' + result[j]['Account ID'] + ',' + result[j]['Adbase Account ID']  + ',' +result[j]['Account Name'] + ',' + result[j]['Billing Street'] + ',' + result[j]['Billing City'] + ',' + result[j]['Billing State/Province'] + ',' + result[j]['Billing Zip/Postal Code'] ;
					sfDetails.push(result[j]['Account ID'] + ',' + result[j]['Adbase Account ID']  + ',' +result[j]['Account Name'] + ',' + result[j]['Billing Street'] + ',' + result[j]['Billing City'] + ',' + result[j]['Billing State/Province'] + ',' + result[j]['Billing Zip/Postal Code']);
				}	

				logString += '"';
	
				//console.log(logString );
			}
			else
			{
				acctStatus = 'FOUND';
				adbaseAccountNo  = AdbaseNo;
				adbaseDetails =  accountName1 +  ',' + accountName2 + ',' + billingStreet + ',' + city + ',' + zip;		
				sfDetails.push(result[0]['Account ID'] + ',' + result[0]['Adbase Account ID']  + ',' + result[0]['Account Name'] + ',' + result[0]['Billing Street'] + ',' + result[0]['Billing City'] + ',' + result[0]['Billing State/Province'] + ',' + result[0]['Billing Zip/Postal Code']);
				
				//console.log('FOUND,"'  +  AdbaseNo  +'","' + accountName1 +  ',' + accountName2 + ',' +  billingStreet + ',' + city + ','  + zip +  '","' + result[0]['Account ID'] + ',' + result[0]['Adbase Account ID']  + ',' + result[0]['Account Name'] + ',' + result[0]['Billing Street'] + ',' + result[0]['Billing City'] + ',' + result[0]['Billing State/Province'] + ',' + result[0]['Billing Zip/Postal Code']+'"');		   
			}				
		} 
		else if(result.length == 0)
		{
			acctStatus = 'NOT FOUND';
			adbaseAccountNo  = AdbaseNo;
			adbaseDetails =  accountName1 +  ',' + accountName2 + ',' + billingStreet + ',' + city + ',' + zip;		
			sfDetails.push('');
						
			//console.log('NOT FOUND,"'  + AdbaseNo  + '","' + accountName1 + ','  + accountName2 + ',' + billingStreet+ ',' + city + ',' + zip + '"');
		}	
					
		var docToInsert = {STATUS : acctStatus,
									'Adbase Account No' : adbaseAccountNo,
									'Adbase Account Details' : adbaseDetails,
								'SF Account Details' : sfDetails};
														
		insertInDB(db,docToInsert,inputCollection);
	});
}

var findAccountsInAdbasedAcctNameMatch = function(db)
{
	var cursor = db.collection(tableName).find({TYPEID : {$ne:4}});
   
   cursor.toArray(function (err, documentArray)  
   {  		
		assert.equal(err, null);

		if (err)
		{
			console.log(err);
			
			db.close();
		} 
		else if (documentArray.length) 
		{
			totalNumberOfDocsToProcess += documentArray.length ;
			
			for(var i = 0 ; i < documentArray.length ; i++)
				{				
					var accountName1 = documentArray[i]['NAME1'];
					var accountName2 = documentArray[i]['NAME2'];
					
					var billingStreet = documentArray[i]['PRIMARYADDRESS1'];
					var city = documentArray[i]['PRIMARYCITY'];
					var state = documentArray[i]['STATE'];
					var zip = documentArray[i]['PRIMARYZIPCODE'];
					var AdbaseNo = documentArray[i]['ACCOUNTNUMBER'];
					
					if((accountName1 || accountName2) && billingStreet)
					{
						var cursor1 = db.collection('salesforceaccounts').find( {$or : [{'Account Name' :  accountName1}, {'Account Name' : accountName2}]});
						
						searchInSalesforce(db,accountName1,accountName2,billingStreet,city,zip,AdbaseNo,cursor1,'AccountNameMatch');
					}
					else
					{
						//console.log('INVALID ACCOUNT,"' + AdbaseNo  + '","'  + accountName1 +  ',' + accountName2 + ',' + billingStreet + ',' + city  + ',' + zip + '"');
				
						acctStatus = 'INVALID ACCOUNT';
						adbaseDetails =  accountName1 +  ',' + accountName2 + ',' + billingStreet + ',' + city + ',' + zip;		
						
						var sfDetails = new Array();
								
						var docToInsert = {STATUS : 'INVALID ACCOUNT',
															'Adbase Account No' : AdbaseNo,
															'Adbase Account Details' : adbaseDetails,
															'SF Account Details' : sfDetails};
															
						insertInDB(db,docToInsert,'AccountNameMatch');
				
					}
				}	
		}
		else 
		{
			console.log('No document(s) found with defined "find" criteria!');
			db.close();	
		}
	  
   });	
	
};

var findAccountsInAdbasedBillingAddrMatch = function(db)
{
	var cursor = db.collection(tableName).find({TYPEID : {$ne:4}});
   
   cursor.toArray(function (err, documentArray)  
   {  		
		assert.equal(err, null);

		if (err)
		{
			console.log(err);
			
			db.close();
		} 
		else if (documentArray.length) 
		{	
			totalNumberOfDocsToProcess += documentArray.length ;
			
			for(var i = 0 ; i < documentArray.length ; i++)
			{				
				var accountName1 = documentArray[i]['NAME1'];
				var accountName2 = documentArray[i]['NAME2'];
				
				var billingStreet = documentArray[i]['PRIMARYADDRESS1'];
				var city = documentArray[i]['PRIMARYCITY'];
				//var state = documentArray[i]['STATE'];
				var zip = documentArray[i]['PRIMARYZIPCODE'];
				var AdbaseNo = documentArray[i]['ACCOUNTNUMBER'];
				
				if((accountName1 || accountName2) && billingStreet)
				{
					var cursor1 = db.collection('salesforceaccounts').find({'Billing Street' : billingStreet,
																		   'Billing City' : city,
																		   'Billing Zip/Postal Code':zip
																			});
					
					searchInSalesforce(db,accountName1,accountName2,billingStreet,city,zip,AdbaseNo,cursor1,'BillingAddrMatch');
				}
				else
				{
					//console.log('INVALID ACCOUNT,"' + AdbaseNo  + '","'  + accountName1 +  ',' + accountName2 + ',' + billingStreet + ',' + city  + ',' + zip + '"');
			
					acctStatus = 'INVALID ACCOUNT';
					adbaseDetails =  accountName1 +  ',' + accountName2 + ',' + billingStreet + ',' + city + ',' + zip;		
					
					var sfDetails = new Array();
			
					var docToInsert = {STATUS : 'INVALID ACCOUNT',
														'Adbase Account No' : AdbaseNo,
														'Adbase Account Details' : adbaseDetails,
														'SF Account Details' : sfDetails};
														
					insertInDB(db,docToInsert,'BillingAddrMatch');

				}
			}	
		}
		else 
		{
			console.log('No document(s) found with defined "find" criteria!');
			db.close();	
		}
   });
};

var findAccountsInAdbasedAcctBillingAddrMatch = function(db)
{
	var cursor = db.collection(tableName).find({TYPEID : {$ne:4}});
   
   cursor.toArray(function (err, documentArray)  
   {  		
		assert.equal(err, null);

		if (err)
		{
			console.log(err);
			
			db.close();
		} 
		else if (documentArray.length) 
		{
			totalNumberOfDocsToProcess += documentArray.length;			
			
			for(var i = 0 ; i < documentArray.length ; i++)
			{				
				var accountName1 = documentArray[i]['NAME1'];
				var accountName2 = documentArray[i]['NAME2'];
				
				var billingStreet = documentArray[i]['PRIMARYADDRESS1'];
				var city = documentArray[i]['PRIMARYCITY'];
				var state = documentArray[i]['STATE'];
				var zip = documentArray[i]['PRIMARYZIPCODE'];
				var AdbaseNo = documentArray[i]['ACCOUNTNUMBER'];
				
				if((accountName1 || accountName2) && billingStreet)
				{
					var cursor1 = db.collection('salesforceaccounts').find( {$or : [{'Account Name' :  accountName1}, {'Account Name' : accountName2}],
																			'Billing Street' : billingStreet,
																			'Billing City' : city,
																			'Billing Zip/Postal Code':zip
																			});
					
					searchInSalesforce(db,accountName1,accountName2,billingStreet,city,zip,AdbaseNo,cursor1,'AcctNameBillingAddrMatch');
				}
				else
				{
					//console.log('INVALID ACCOUNT,"' + AdbaseNo  + '","'  + accountName1 +  ',' + accountName2 + ','+  billingStreet + ',' + city  + ',' + zip + '"');
					
					acctStatus = 'INVALID ACCOUNT';
					adbaseDetails =  accountName1 +  ',' + accountName2 + ',' + billingStreet + ',' + city + ',' + zip;		
					
					var sfDetails = new Array();
					
					var docToInsert = {STATUS : 'INVALID ACCOUNT',
														'Adbase Account No' : AdbaseNo,
														'Adbase Account Details' : adbaseDetails,
														'SF Account Details' : sfDetails};
														
					insertInDB(db,docToInsert,'AcctNameBillingAddrMatch');
								
				}
			}
		}
		else 
		{
			console.log('No document(s) found with defined "find" criteria!');
			db.close();	
		}
   });		
			
};

var findDuplicatesInAdbase = function(db)
{
	var cursor = db.collection(tableName).aggregate([
				{$group: { _id: { firstField: "$NAME1", secondField: "$PRIMARYADDRESS1",thirdField : "$PRIMARYADDRESS2",fourthField : "$PRIMARYCITY" , fifthField :"$PRIMARYZIPCODE" ,sixthField:"$NAME2"}, AdbaseAccountNos: { $addToSet: "$ACCOUNTNUMBER" },count: { $sum:1 } }}, 
				{$match: {  count: { $gt: 1 }}} ,
				{$project:{"NAME1" : "$_id.firstField", "NAME2" : "$_id.sixthField" ,"PRIMARYADDRESS1" : "$_id.secondField", "PRIMARYADDRESS2" : "$_id.thirdField","CITY" : "$_id.fourthField","PRIMARYZIPCODE" : "$_id.fifthField","_id" : 1,"AdbaseAccountNos" : 1, "count":1}}]);
	 
   cursor.toArray(function (err, result)  
   {  		
		assert.equal(err, null);

		if (err)
		{
			console.log(err);
		} 
		else if (result.length) 
		{
			db.collection('AdbaseDuplicates').insert(result);
		}
		else 
		{
			console.log('No document(s) found with defined "find" criteria!');
		}
	  
   });	
};

var insertInDB = function(db,doc,collName)
{
	db.collection(collName).insert(doc,function(err,result)
	{
		if (err)
		{
			console.log(err);
		} 
		else 
		{
			noOfDocsProcessed++;
			
			if(noOfDocsProcessed == totalNumberOfDocsToProcess)
			{
				db.close();
			}
		}	
	});
};
