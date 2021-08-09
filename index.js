const csv = require("fast-csv");
const mysql = require("mysql");
const aws = require("aws-sdk");
const s3 = new aws.S3({ apiVersion: "22006-03-01" });
let obj = {
    key: undefined,
    bucket: undefined,
    does_batch_exist: undefined,
    does_file_exist: undefined,
    does_patient_exist: undefined,
    file_identifier: undefined,
    batch_identifier: undefined,
    file_batch_linking_id: undefined,
    patient_id: undefined,
    medherent_id: undefined,
    patient_identifier: undefined,
    device_id: undefined,
    batch_number: undefined,
    bag_numbers: [],
    csv_rows: []
};
const con = mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
});
const read_only_con = mysql.createConnection({
    host: "mh-db-server-read-replica.cgnilmixhass.us-east-1.rds.amazonaws.com",
    user: "Joe",
    password: "2b008ae3-6780-4da4-9dd5-449b7e12fa63",
    database: "medherent"
});
async function GetMedherentID() {
    read_only_con.query(
        "select cu.user_id from medherent.consumer_users cu join users u on cu.user_id = u._id where cu.patient_code = ? and u.deactivated = 0",
        [obj.patient_id],
        (error, results) => {
            if (error) throw error;
            console.log(results);
            if (results.length == 0) {
                console.log("No medherent_id found. stopping now");
                exit();
            }
            obj.medherent_id = results[0].user_id;
            DoesFileExist();
            
        }
    )
}
async function DoesFileExist() {
    con.query(
        "select count(id) as count from files where s3_key = ?",
        [obj.key],
        (error, results) => {
            if (error) throw error;
            console.log(results);
            obj.does_file_exist = results[0].count;
            if (obj.does_file_exist > 0) {
                console.log("File exists, stopping now");
                throw new Error();
            }
            AddNewFile();
        }
    );
}
async function AddNewFile() {
    con.query(
        "insert into files (s3_key) values (?)",
        [obj.key],
        (error, results) => {
            if (error) throw error;
            console.log(results);
            obj.file_identifier = results.insertId;
            DoesPatientExist()
        }
    );
}
async function DoesPatientExist() {
    con.query(
        "select count(id) as count from patients where patient_id = ?",
        [obj.patient_id],
        (error, results) => {
            if (error) throw error;
            console.log(results);
            obj.does_patient_exist = results[0].count;
            if (obj.does_patient_exist > 0) {
                UpdateExistingPatient();
                GetExistingPatientIdentifier();
            } else {
                AddNewPatient();
            }
        }
    );
}
async function AddNewPatient() {
    con.query(
        "insert into patients (patient_id, device_id, medherent_id) values (?, ?, ?)",
        [obj.patient_id,obj.device_id,obj.medherent_id],
        (error, results) => {
            if (error) throw error;
            obj.patient_identifier = results.insertId;
            DoesBatchExist();
        }
    );
}
async function UpdateExistingPatient() {
    con.query(
        "update patients set device_id = ?, updated_at = CURRENT_TIMESTAMP() where patient_id = ?",
        [obj.device_id,obj.patient_id],
        (error, results) => {
            if (error) throw error;
            console.log(results);
        }
    );
}
async function GetExistingPatientIdentifier() {
    con.query(
        "select id from patients where patient_id = ?", 
        [obj.patient_id],
        (error, results) => {
            if (error) throw error;
            console.log(results);
            obj.patient_identifier = results[0].id;
            DoesBatchExist();
        }
    );
}
async function DoesBatchExist() {
    con.query(
        "select count(id) as count from batches where batch_number = ?",
        [obj.batch_number],
        (error, results) => {
            if (error) throw error;
            console.log(results);
            obj.does_batch_exist = results[0].count;
            if (obj.does_batch_exist > 0) {
                GetExistingBatchIdentifier();
            } else {
                AddNewBatch();
            }
        }
    );
}
async function AddNewBatch() {
    con.query(
        "insert into batches (batch_number, bag_numbers) values (?, ?)",
        [obj.batch_number,JSON.stringify(obj.bag_numbers)],
        (error, results) => {
            if (error) throw error;
            console.log(results);
            obj.batch_identifier = results.insertId;
            AddNewBatchFileLinking();
        }
    );
}
async function GetExistingBatchIdentifier() {
    con.query(
        "select id from batches where batch_number = ?", 
        [obj.batch_number],
        (error, results) => {
            if (error) throw error;
            console.log(results);
            obj.batch_identifier = results[0].id;
            AddNewBatchFileLinking();
        }
    );
}
async function AddNewBatchFileLinking() {
    con.query(
        "insert into batch_file_linking (batch_id, file_id) values (?, ?)",
        [obj.batch_identifier, obj.file_identifier],
        (error, results) => {
            if (error) throw error;
            console.log(results);
            obj.file_batch_linking_id = results.insertId;
            AddNewLoad();
        }
    );
}
async function AddNewLoad() {
    con.query(
        "insert into loads (patient_id, batch_file_linking_id) values (?, ?)",
        [obj.device_id,obj.file_batch_linking_id],
        (error, results) => {
            if (error) throw error;
            console.log(results);
            obj.load_id = results.insertId;
            console.log(obj);
        }
    );
}
async function run() {
    obj.patient_id = obj.csv_rows[0].PTID;
    obj.device_id = obj.csv_rows[0].DEVICEID;
    obj.batch_number = obj.csv_rows[0].BATCHID;
    obj.bag_numbers = Array.from(new Set(obj.bag_numbers));
    await GetMedherentID();
}
exports.handler = (event, context) => {
    obj.bucket = event.Records[0].s3.bucket.name;
    obj.key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
    let stream = s3.getObject({Bucket: obj.bucket, Key: obj.key}).createReadStream();
    csv.parseStream(stream,{headers: true, escape: "\\", trim: true})
    .on('error', error => console.error(error))
    .on("data", (data) => {
        obj.csv_rows.push(data);
        obj.bag_numbers.push(data.BAGNUMBER);
    })
    .on("end", async () => {
        await run();
    });
}