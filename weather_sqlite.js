const sqlite3 = require('sqlite3').verbose();
const https = require('https');
const http = require("http");

const csv=require('./generate_csv');

const host = '0.0.0.0';
const port = 8000;

const prediction_days=7;

let sql;
let db;
//create/open database
function db_init(){
    //since data used has to be up to date,an in memory db is used since it resets after the program ends
    db = new sqlite3.Database(/*'./weather_forecasts.db'*/':memory:', (err) => {
        if (err) {
            console.error(err.message);
        }
        console.log('Connected to the weather_forecasts database.');
    });

    sql=`CREATE TABLE IF NOT EXISTS locations (
        title TEXT,
        location_type TEXT,
        woeid INT,
        latt_long TEXT,
        PRIMARY KEY (woeid))
    `;

    //create locations table
    db.run(sql,async (err) => {
        if (err) {
            console.error(err.message);
        }
       

        sql=`CREATE TABLE IF NOT EXISTS forecasts (
            forecast_id INTEGER NOT NULL,
            weather_state_name TEXT,
            weather_state_abbr TEXT,
            wind_direction_compass TEXT,
            created DATETIME,
            applicable_date DATE,
            min_temp INTEGER,
            max_temp INTEGER,
            the_temp INTEGER,
            wind_speed REAL,
            wind_direction REAL,
            air_pressure REAL,
            humidity REAL,
            visibility REAL,
            predictability INTEGER,
            woeid INTEGER,
            PRIMARY KEY (forecast_id),
            FOREIGN KEY(woeid) REFERENCES locations(woeid))
        `;
        
        //create forecats table and fill both
        db.run(sql, (err) => {
            if (err) {
                console.error(err.message);
            }

            fill_tables();
        });
    });
}

var options = {
    method: 'GET',
    host: 'www.metaweather.com',
    path: '/api/location/44418/2021/6/10/'
};

async function empty_tables(){
    await db.serialize(() => {
        // queries will execute in serialized mode
        let sql=(`DELETE FROM locations`);
        db.run(sql, (err) => {
            if (err) {
                console.error(err.message);
            }
        });
        sql=(`DELETE FROM forecasts`);
        db.run(sql, (err) => {
            if (err) {
                console.error(err.message);
            }
        });
    });
}


function fill_tables(){

    //generate data needed for queries
    let locations=['london','berlin','moscow'];
    let dates=[];
    var datetime = new Date();
    console.log(datetime);
    for(let i=0;i<prediction_days;i++){
        datetime.setDate(datetime.getDate()+1);
        dates[i]=datetime.getFullYear()+'/'+(datetime.getMonth()+1)+'/'+datetime.getDate()+'/';
    }

    locations.forEach(location => {
        options["path"]='/api/location/search/?query='+location;
        get_location_data(options,dates);
    });

}

function get_location_data(options,dates){
    let request = https.request(options, (res) => {
        if (res.statusCode !== 200) {
            console.error(`Did not get an OK from the server. Code: ${res.statusCode}`);
            res.resume();
            return;
        }
        let data = '';

        res.on('data', (chunk) => {
            data += chunk;
        });

        res.on('close', () => {
            console.log('Retrieved location data');
            store_location_data(JSON.parse(data),dates);
        });
    });
    
    request.end();

    request.on('error', (err) => {
        console.error(`Encountered an error trying to make a request: ${err.message}`);
    });
}

function store_location_data(data,dates){
    data.forEach( location => {

        let sql=(`INSERT INTO locations 
        (title,
            location_type,
            woeid,
            latt_long) 
        VALUES
        ('${location['title']}'
           ,'${location['location_type']}'
           ,${location['woeid']}
           ,'${location['latt_long']}');
        `);

        db.run(sql,[], (err) => {
            if(err!=null){
                console.error(`Encountered an error ${err.message}`);
            }

            dates.forEach(date => {
                options["path"]='/api/location/'+location['woeid']+'/'+date;
                get_forecastdata(options,location['woeid']);
            });
        });
    });
}


function get_forecastdata(options,location){
    let request = https.request(options, (res) => {
        if (res.statusCode !== 200) {
            console.error(`Did not get an OK from the server. Code: ${res.statusCode}`);
            res.resume();
            return;
        }
        let data = '';
    
        res.on('data', (chunk) => {
            data += chunk;
        });
    
        res.on('close', () => {
            console.log('Retrieved forecast data');

            store_forecast_data(JSON.parse(data),location);
        });
    });
    
    request.end();

    request.on('error', (err) => {
        console.error(`Encountered an error trying to make a request: ${err.message}`);
    });
}

function store_forecast_data(data,location){
    data.forEach(async forecast => {

        let sql=(`INSERT INTO forecasts 
        (forecast_id
            , weather_state_name
            , weather_state_abbr
            , wind_direction_compass
            , created
            , applicable_date
            , min_temp
            , max_temp
            , the_temp
            , wind_speed
            , wind_direction
            , air_pressure
            , humidity
            , visibility
            , predictability
            , woeid) 
        VALUES
        (${forecast['id']}
           ,'${forecast['weather_state_name']}'
           ,'${forecast['weather_state_abbr']}'
           ,'${forecast['wind_direction_compass']}'
           ,'${forecast['created']}'
           ,'${forecast['applicable_date']}'
           ,${forecast['min_temp']}
           ,${forecast['max_temp']}
           ,${forecast['the_temp']}
           ,${forecast['wind_speed']}
           ,${forecast['wind_direction']}
           ,${forecast['air_pressure']}
           ,${forecast['humidity']}
           ,${forecast['visibility']}
           ,${forecast['predictability']}
           ,${location});
        `);

        db.run(sql,[], (err) => {
            if(err!=null){
                console.error(`Encountered an error ${err.message}`);
            }
        });
    });
}

//executes sql query and sends response
function prepare_send_response(sql,res){
    db.all(sql, (err, rows) => {
        if (err) {
            throw err;
        }

        let data=JSON.stringify(rows);
        res.setHeader('Content-Type', 'application/json');
        res.writeHead(200);
        res.end(data);
    });
}


function latest(res){
    let sql=`SELECT forecast_id
    , weather_state_name
    , weather_state_abbr
    , wind_direction_compass
    , max(created) as created
    , applicable_date
    , min_temp
    , max_temp
    , the_temp
    , wind_speed
    , wind_direction
    , air_pressure
    , humidity
    , visibility
    , predictability
    , title
    FROM forecasts LEFT JOIN locations on forecasts.woeid=locations.woeid
    GROUP BY applicable_date,forecasts.woeid 
    ORDER BY forecasts.woeid DESC,applicable_date`;

    prepare_send_response(sql,res);
    console.log("responded /latest");
}

function avgtemp(res){
    let sql=`SELECT title
    ,applicable_date
    , avg(the_temp)
    FROM (SELECT title,applicable_date,the_temp,row_number() OVER (PARTITION BY forecasts.woeid,applicable_date ORDER BY created DESC) as rownum FROM forecasts LEFT JOIN locations on forecasts.woeid=locations.woeid)
    WHERE rownum<=3 
    GROUP BY applicable_date,title 
    `;

    prepare_send_response(sql,res);
    console.log("responded /avgtemp");
}

function toploc(n,res){
    let sql=`SELECT * FROM(SELECT * FROM (SELECT * FROM forecasts ORDER BY created DESC LIMIT ${n})
    UNION ALL
    SELECT * FROM (SELECT * FROM forecasts ORDER BY min_temp ASC LIMIT ${n})
    UNION ALL
    SELECT * FROM ( SELECT * FROM forecasts ORDER BY max_temp DESC LIMIT ${n})
    UNION ALL
    SELECT * FROM (SELECT * FROM forecasts ORDER BY the_temp DESC LIMIT ${n})
    UNION ALL
    SELECT * FROM (SELECT * FROM forecasts ORDER BY wind_speed DESC LIMIT ${n})
    UNION ALL
    SELECT * FROM (SELECT * FROM forecasts ORDER BY air_pressure DESC LIMIT ${n})
    UNION ALL
    SELECT * FROM (SELECT * FROM forecasts ORDER BY humidity DESC LIMIT ${n})
    UNION ALL
    SELECT * FROM (SELECT * FROM forecasts ORDER BY visibility DESC LIMIT ${n})
    UNION ALL
    SELECT * FROM (SELECT * FROM forecasts ORDER BY predictability DESC LIMIT ${n})) as forecasts
    LEFT JOIN locations ON forecasts.woeid=locations.woeid`;

    prepare_send_response(sql,res);
    console.log("responded /toploc");
}


const  requestListener =async function (req, res) {

    var url_parts=new URL(req.url,req.protocol + '://' + req.headers.host + '/');

    switch(url_parts.pathname){
        case '/csv':
            let v=await csv.generate_csv(db);
            res.writeHead(v.code);
            res.end(v.text);
            console.log('responded /csv');
            console.log(v);
        break;
        case '/upToDate':
            await empty_tables();
            fill_tables();
            console.log('responded /upToDate');
            res.writeHead(200);
            res.end('database is being updated');
        break;
        case '/latest':
            latest(res);
        break;
        case '/avgTemp':
            avgtemp(res);
        break;
        case '/topLoc':
            let n=parseInt(url_parts.searchParams.get('n'));
            if(!isNaN(n)&&n>=0){
                toploc(n,res);
                break;
            }
        default:
            res.writeHead(200);
            res.end(`You can choose what operation to perform by placing any of the below at the end of the ulr
            /latest :To list the latest forecast for each location for every day
            /avgTemp :To list the average the_temp of the last 3 forecasts for each location for every day
            /Toploc?n=value :To get the top n locations based on each available metric where value is a positive integer
            /csv :To generate csv files containing the sql queries used and table content
            /upToDate :To update information in the database to the most recent forecasts
            `);
    }
};


//starts server
const server = http.createServer(requestListener);
server.listen(port, host, () => {
  console.log(`Server is running on port :${port}`);
});

//makes database and fetches all data needed
db_init();







/*db.close((err) => {
    if (err) {
      console.error(err.message);
    }
    console.log('Closed the database connection.');
});*/


