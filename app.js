require('dotenv').config()
const fs = require("fs")
const writeFilSync = fs.writeFile
const readline = require('readline');
const source_data = process.env.INPUT_FILENAME

const readInterface = readline.createInterface({
    input: fs.createReadStream(source_data),
    console: false
});
let line_count = 0
let local_line_count = 0
let file_number = 0

//A syncrous file write is used as the file order matters
const wf = ( file_number, line) => {
    try {
        let file_name = process.env.OUTPUT_FILENAME + `_${file_number}.json`
        console.log(file_name);
        fs.writeFileSync(file_name,line + "\n",{flag:"a"} )
    } catch (err) {
        console.log(err);
    }
}

readInterface.on('line', function(line) {
    //console.log(line);
    if ( line_count > 1 ){
        if ( local_line_count === 0 ){
            //Create a new file
            wf(file_number,'{"schemaVersion":"796","build":"445","start":true,"majorVersion":"5","minorVersion":"4","revision":"0"}')
            wf(file_number,`{"name":"${process.env.WWX_STREAMNAME}","type":"Stream"}`)
            wf(file_number,line)
            local_line_count++
            console.log(`Processing batch ${file_number}`);
        } else if ( local_line_count === 500000){
            //Wire the last line unless it is the last line
            wf(file_number,line)
            if (line !== '{"end":true}'){  //Closes the file
                wf(file_number,'{"end":true}')
            } 
            local_line_count = 0 //Start a new file on the next loop
            file_number++
        } else {
            wf(file_number,line)
            local_line_count++
        }
    }
    line_count++
});

readInterface.on('close', () => {
    console.log(`Processing Finished ${line_count}`);
    console.log("Done");
});

