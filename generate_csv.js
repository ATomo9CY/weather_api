const fs = require("fs").promises;

async function writeFile(file_name,csv_content) {

    try {
        await fs.writeFile(file_name, csv_content);
    } catch (error) {
        console.error(`Got an error trying to write a file: ${error.message}`);
        return {code:500,text:`Got an error trying to write a file: ${error.message}`};
    }
}


exports.generate_csv=async (db)=>{
    let fnames=[];
    let fcontent=[];
    fnames[fnames.length]='generate_forecasts_table_sql.csv';
    fcontent[fcontent.length]=`CREATE TABLE IF NOT EXISTS forecasts (
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
        PRIMARY KEY (forecast_id));
        FOREIGN KEY(woeid) REFERENCES locations(woeid))
    `;

    fnames[fnames.length]='insert_into_forecasts_table_sql.csv';
    fcontent[fcontent.length]=`INSERT INTO forecasts 
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
    (\${forecast['id']}
       ,'\${forecast['weather_state_name']}'
       ,'\${forecast['weather_state_abbr']}'
       ,'\${forecast['wind_direction_compass']}'
       ,'\${forecast['created']}'
       ,'\${forecast['applicable_date']}'
       ,\${forecast['min_temp']}
       ,\${forecast['max_temp']}
       ,\${forecast['the_temp']}
       ,\${forecast['wind_speed']}
       ,\${forecast['wind_direction']}
       ,\${forecast['air_pressure']}
       ,\${forecast['humidity']}
       ,\${forecast['visibility']}
       ,\${forecast['predictability']}
       ,\${location});
    `;

    fnames[fnames.length]='query_1.csv'
    fcontent[fcontent.length]=`SELECT forecast_id
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

    fnames[fnames.length]='query_2.csv';
    fcontent[fcontent.length]=`SELECT title
    ,applicable_date
    , avg(the_temp)
    FROM (SELECT title,applicable_date,the_temp,row_number() OVER (PARTITION BY forecasts.woeid,applicable_date ORDER BY created DESC) as rownum FROM forecasts LEFT JOIN locations on forecasts.woeid=locations.woeid)
    WHERE rownum<=3 
    GROUP BY applicable_date,title
    `;

    fnames[fnames.length]='query_3.csv';
    fcontent[fcontent.length]=``;

    let fl=fnames.length;
    fnames[fnames.length]='forcasts_table_content.csv';
    fcontent[fcontent.length]=`forecast_id,weather_state_name,weather_state_abbr,wind_direction_compass,created,applicable_date,min_temp,max_temp,the_temp,wind_speed,wind_direction,air_pressure,humidity,visibility,predictability,woeid`;

    db.all('SELECT * FROM forecasts', (err, rows) => {
        if (err) {
            return {code:500,text:`error ${err.message}`};
        }
       
        rows.forEach((row) => {
            let data='';
            for (const [key, value] of Object.entries(row)) {
                data=data+value+',';
            }
            fcontent[fl]=fcontent[fl]+'\n'+data;
        });
    });

    fnames[fnames.length]='generate_locations_table_sql.csv';
    fcontent[fcontent.length]=`CREATE TABLE IF NOT EXISTS locations (
        title TEXT,
        location_type TEXT,
        woeid INT,
        latt_long TEXT,
        PRIMARY KEY (woeid))
    `;


    let ll=fnames.length;
    fnames[fnames.length]='locations_table_content.csv';
    fcontent[fcontent.length]=`title,location_type,woeid,latt_long`;

    
    db.all('SELECT * FROM locations',(err, rows) => {
        if (err) {
            return {code:500,text:`error ${err.message}`};
        }
       
        rows.forEach((row) => {
            let data='';
            for (const [key, value] of Object.entries(row)) {
                data=data+value+',';
            }
            fcontent[ll]=fcontent[ll]+'\n'+data;
        });
    });
    

    fnames[fnames.length]='insert_into_locations_table.csv';
    fcontent[fcontent.length]=`INSERT INTO locations 
    (title,
        location_type,
        woeid,
        latt_long) 
    VALUES
    ('\${location['title']}'
       ,'\${location['location_type']}'
       ,\${location['woeid']}
       ,'\${location['latt_long']}');
    `;

    
    for(let i=0;i<fnames.length;i++){
       await writeFile(fnames[i],fcontent[i]);
    }
    return {code:200,text:`files generated succesfully`};
}