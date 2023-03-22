require('dotenv').config();
const { MongoClient } = require('mongodb');
const mysql = require('mysql2');
const countries = require('./countries');
const generes = require('./generes');

/*

    STEP BY STEP

    1.- connect to mongoDB.
    2.- Get all documents from movies collection and only the fields we want.
    3.- Process all documents to divide data.




    NOTES:
    REALIZA A RESEARCH ABOUT THE BEST WAY TO CONNECT TO FILL A ENTITIE WHEN THERE IS A M TO N RELASHIONSHIP.

*/


class MongoDBDatabase
{
    constructor()
    {
        this.client = new MongoClient(process.env.MONGOCONNECTIONSTRING);
        this.collection = this.client.db('sample_mflix').collection('movies');
    }
    getMoviesData = () =>
    new Promise(async (resolve, reject) => {
        this.client.connect();
        const pipeline = [
            {
               $project: {
                    _id: 0, title: 1, rated: 1, year: 1,  
                }
            },
            {
                $limit: 10
            }
        ]
        try
        {
            const moviesData = await this.collection.aggregate(pipeline).toArray();
            resolve(moviesData);
        }
        catch (e)
        {
            reject(e)
            console.error(e)
        }
        finally 
        {
            this.client.close();
        }
    })
}

class MysqlDataBase
{
    constructor()
    {
        this.connection = mysql.createConnection({
            host: process.env.MYSQLHOST,
            password: process.env.MYSQLPASSWORD,
            user: process.env.MYSQLUSER,
            database: process.env.MYSQLDATABASE
        });
        const ratedCategories = [];
    }
    loadRatedCategories = async () => {
        try
        {
            console.log('loading rated categories');
        }
        catch(err)
        {
            console.log(err)
        }
    }
    startConnection = () => 
        new Promise((resolve, reject) => {
            this.connection.connect((err) => {
                if (err) {
                    reject(err);
                } else {
                    resolve('connected');
                }
            })
        });
    
    closeConnection = () =>
        new Promise((resolve, reject) => {
            this.connection.end((err) => {
                if (err) {
                    reject(err);
                } else {
                    resolve('closed');
                }
            })
        });
}

(async ()=>{
    const mongoDB = new MongoDBDatabase();
    const moviesData = await mongoDB.getMoviesData();
    console.table(moviesData);
})();

/*
    DEFINE MYSQL CONFIG
*/

const mysqlConfig = {
    host: process.env.MYSQLHOST,
    password: process.env.MYSQLPASSWORD,
    user: process.env.MYSQLUSER,
    database: process.env.MYSQLDATABASE
}


async function main (){
    // start connection
    const connection = mysql.createConnection(mysqlConfig);
    //connect to mongoDB
    await client.connect();

    try
    {
        
        await connection.connect((err) => {
            if(err) {
                console.log(`Error connecting to ${mysqlConfig.database}`)
            } else {
                console.log(`connected to ${mysqlConfig.database}`)
            }
        })

        // get and load rated fields
        console.log('getting rated categories');
        await loadRatedCategories(connection);    
        await uploadCountries(connection);
    }
    catch(error)
    {
        console.log(error);
    }
    finally
    {
        // close mongodb connection
         await client.close();
        // close mysql conenction
        connection.end();
    }
}
// main();


function getRatedCategories() {
    // print process
   
}

async function loadRatedCategories(connection) {
    // print process
    console.log('uploading rated categories');
    try {
        //get rated categories
        const ratedCategories = await getRatedCategories();
        //load rated categories
        let bigQuery = 'INSERT INTO clasificaciones (clasificacion) VALUES ';
        ratedCategories.forEach((ratedCategory, index) => {
            bigQuery += `('${ratedCategory}')`;
            if (index < ratedCategories.length - 1) {
                bigQuery += ',';
            }
        })
        bigQuery += ';';

        // execute query
        connection.query(bigQuery, (error, results) => {
            if (error)
            {
                console.log(error)
                console.log("Error loading ratedcategories to mysql")
                return;
            }
            console.log(results);
        });

    } catch (error) {
        console.log(error)
        console.log("Error loading ratedcategories to mysql")
    }
    finally
    {
        console.log('Rated categories has been loaded')
    }
}

async function uploadCountries(connection) {
        try
        {
            const countriesToInsert = await getCountriesAvailable();
            let bigQuery = "INSERT INTO paises (pais) VALUES ";
            countriesToInsert.forEach((country, index) => {
                bigQuery += `("${country}")`;
                if (index < countriesToInsert.length - 1) {
                    bigQuery += ',';
                }
            })
            bigQuery += ';';

            // execute query
            connection.query(bigQuery, (error, results) => {
                if (error)
                {
                    console.log(error)
                    console.log("Error loading countries to mysql")
                    return;
                }
                console.log(results);
            });
        }
        catch(err)
        {
            console.log(err)
        }
        finally
        {
            console.log('done')
        }
}


function getCountriesAvailable()
{
    return new Promise(async (resolve, reject) => {
        try
        {
            const allCountries = await collection.find({}).project({ _id: 0, countries: 1 }).toArray();
            // console.log(allCountries);
            let = countriesResume = [];
            allCountries.forEach((country) => {
                if(!country.countries)
                {
                    return;
                }

                if (!country.countries[0]) {
                    return;
                }
             
                if (!countriesResume.includes(country.countries[0])) {
                    countriesResume = [...countriesResume, country.countries[0]]
                }
            });
            resolve(countriesResume);
            console.log(countriesResume)
        }
        catch(err)
        {
            reject(err)
            console.log(err)
        }
        finally
        {
            console.log('done')
        }
    });
}


function getMovies()
{
    return new Promise(async (resolve, reject) => {
        try
        {
            const movies = await collection.find({}).project({title:1, year:1,_id: 0, countries:1, rated:1 }).toArray();
            resolve(movies);
        }
        catch(err)
        {
            reject(err)
        }
        finally
        {
            console.log('done')
        }
    });
}

// getMovies();


async function loadMoviesToMySQL(connection)
{
    try {
        //get paises from mysql
        const connection = mysql.createConnection(mysqlConfig);
        let paises = await getCountriesTableValues();
        let rated = await getRatedTableValues();
        //get movies from mongoDB
        const movies = await getMovies();


        //load movies to mysql
        let bigQuery = "INSERT INTO peliculas (nombre, año, duracion, id_pais, id_clasificacion) VALUES ";
        movies.forEach((movie, index) => {
           
            // ->  change this concatenations for a switch
           
            // if (!movie.rated) {
            //     movie.rated = 'Not Rated';
            // }
            // if( !movie.title)
            // {
            //     return;
            // }
            // if(!movie.countries){
            //     return;
            // }
            // if(typeof movie.year === "string")
            // {
            //     movie.year = 1991;
            // }
            // if(movie.year<=1901)
            // {
            //     movie.year = new Date().getFullYear();
            // }

            switch(movie)
            {
                case !movie.title:
                    movie.rated = 'Not Rated';
                    break;
                case !movie.countries:
                break;
            }

            // generar número aleatorio para las horas (entre 0 y 23)
const horas = Math.floor(Math.random() * 3);

// generar número aleatorio para los minutos (entre 0 y 59)
const minutos = Math.floor(Math.random() * 60);

// generar número aleatorio para los segundos (entre 0 y 59)
const segundos = Math.floor(Math.random() * 60);
// construir cadena de tiempo en formato HH:mm:ss
const timeString = `${horas.toString().padStart(2, '0')}:${minutos.toString().padStart(2, '0')}:${segundos.toString().padStart(2, '0')}`;

            parseInt(movie.year)
            bigQuery += `("${movie.title}", ${movie.year},${`"${timeString}"`},${paises.find(pais => pais.pais === movie.countries[0]).pais_id}, ${rated.find(rated => rated.clasificacion === movie.rated).clasificacion_id})`;
            if (index < movies.length - 1) {
                bigQuery += ',';
            }
        })
        bigQuery += ';';

        // execute query
        connection.query(bigQuery, (error, results) => {
            if (error)
            {
                console.log(error)
                console.log("Error loading movies to mysql")
                return;
            }
            // console.log(results);
        });
    }
    catch(err)
    {
        console.log(err)
    }
}
// loadMoviesToMySQL();


async function getRatedTableValues()
{
    return new Promise((resolve, reject) => {
        try
        {
            const connection = mysql.createConnection(mysqlConfig);
            connection.query('SELECT * FROM clasificaciones', (error, results) => {
                if (error)
                {
                    console.log(error)
                    console.log("Error loading ratedcategories to mysql")
                    return;
                }
                resolve(results);
            })
        }
        catch(err)
        {
            reject(err)
        }
    })
}

async function getCountriesTableValues()
{
    return new Promise((resolve, reject) => {
        try
        {
            const connection = mysql.createConnection(mysqlConfig);
            connection.query('SELECT * FROM paises', (error, results) => {
                if (error)
                {
                    console.log(error)
                    console.log("Error loading ratedcategories to mysql")
                    return;
                }
                resolve(results);
            })
        }
        catch(err)
        {
            reject(err)
        }
    })
}