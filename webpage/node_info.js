/*
Main JavaScript file to read the information from the artsXXX.js and put it into the table.
*/

function get_node_info(node_json, node_id) {
    var json_string, obj;

    // Convert variable to JSON object:
    json_string = JSON.stringify(node_json);
    obj = JSON.parse(json_string);

    // set some style for the node information
    document.getElementById(node_id + "_name").className = "w3-xlarge w3-border-bottom";
    document.getElementById(node_id + "_status").className = "w3-large";
    document.getElementById(node_id + "_process").className = "w3-small";
    document.getElementById(node_id + "_time").className = "w3-small";

    // set content for node
    document.getElementById(node_id + "_name").innerHTML = obj.node_name;
    document.getElementById(node_id + "_status").innerHTML = obj.node_status;
    document.getElementById(node_id + "_process").innerHTML = "Processing: " + obj.node_process;
    document.getElementById(node_id + "_time").innerHTML = "Elapsed time: " + obj.time;

    // set background color for node
    if (obj.node_status === 'running') {
        document.getElementById(node_id).className = 'w3-yellow';
    }
    else if (obj.node_status === 'finished') {
        document.getElementById(node_id).className = 'w3-green';
    }
    else if (obj.node_status === 'failed') {
        document.getElementById(node_id).className = 'w3-red';
    }
    else {
        document.getElementById(node_id).className = 'w3-light-gray';
    }

}


function process_node_list() {
    /*
    Function that goes through all the node variables and the node ids
    */
    console.log('processing node list')
    var node_list = [arts001, arts002, arts003, arts004, arts005, arts006, arts007, arts008, arts009, arts010,
        arts011, arts012, arts013, arts014, arts015, arts016, arts017, arts018, arts019, arts020,
        arts021, arts022, arts023, arts024, arts025, arts026, arts027, arts028, arts029, arts040,
        arts031, arts032, arts033, arts034, arts035, arts036, arts037, arts038, arts039, arts040,
        arts041];
    var id_node_list = ['arts001', 'arts002', 'arts003', 'arts004', 'arts005', 'arts006', 'arts007', 'arts008', 'arts009', 'arts010',
        'arts011', 'arts012', 'arts013', 'arts014', 'arts015', 'arts016', 'arts017', 'arts018', 'arts019', 'arts020',
        'arts021', 'arts022', 'arts023', 'arts024', 'arts025', 'arts026', 'arts027', 'arts028', 'arts029', 'arts030',
        'arts031', 'arts032', 'arts033', 'arts034', 'arts035', 'arts036', 'arts037', 'arts038', 'arts039', 'arts040',
        'arts041'];
    for (i = 0; i < node_list.length; i++) {
        get_node_info(node_list[i], id_node_list[i]);
    }
}

function update_page() {
    /*
    Function to put the information on the webpage including the current time
    */
    process_node_list();
    var date = new Date();
    document.getElementById("date").innerHTML = "Last updated: " + date.toUTCString();
    // setInterval(
    //     function () {
    //         process_node_list();
    //         var date = new Date();
    //         document.getElementById("date").innerHTML = "Last updated: " + date.toUTCString();
    //     }, 5000);
}
