//raw data

var data = 
    [
       {awardee:"A", 'tender_count' :2, avg_comp:4, award_val: 10},
       {awardee:"A", 'tender_count' :2, avg_comp:2, award_val: 40},
       {awardee:"B", 'tender_count' :3, avg_comp:1, award_val: 60},
       {awardee:"B", 'tender_count' :4, avg_comp:4, award_val: 30},
       {awardee:"C", 'tender_count' :5, avg_comp:3, award_val: 20},
    ];


const data_perc = {};

for(var i = 0; i<data.length; i++){

     if (data_perc[data[i]['awardee']]) {

              data_perc[data[i]['awardee']]['tender_count'] = data_perc[data[i]['awardee']]['tender_count'] + Number.parseInt(data[i]['tender_count'])
              data_perc[data[i]['awardee']]['avg_comp']     = data_perc[data[i]['awardee']]['avg_comp'] + Number.parseFloat(data[i]['avg_comp'])
              data_perc[data[i]['awardee']]['award_val']    = data_perc[data[i]['awardee']]['award_val'] + Number.parseFloat(data[i]['award_val'])
              data_perc[data[i]['awardee']]['count']        = data_perc[data[i]['awardee']]['count'] + 1
     }
     else {
	     var temp_obj = {'tender_count':Number.parseInt(data[i]['tender_count']), 'avg_comp':Number.parseFloat(data[i]['avg_comp']), 'award_val':Number.parseFloat(data[i]['award_val']), 'count':1 }; 
	     if (data[i]['awardee']) data_perc[data[i]['awardee']]= temp_obj;
     }

}



// convert the nested array avg_comp value to average and
// Format the data in required shape
for (var key in data_perc) {
    if (Object.prototype.hasOwnProperty.call(data_perc, key)) {         
          data_perc[key]['avg_comp'] = data_perc[key]['avg_comp'] / data_perc[key]['count'] ;
    }
}



const final_res = []

for (var key in data_perc) {
    if (Object.prototype.hasOwnProperty.call(data_perc, key)) {
          
          let temp_array = [data_perc[key]['tender_count'], data_perc[key]['avg_comp'], data_perc[key]['award_val'], key] 
          final_res.push(temp_array)       

      }
        
}




