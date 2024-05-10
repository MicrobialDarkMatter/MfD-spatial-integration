const nodes2ts = require('nodes2ts');

// The function must take as input an S2 cell and a distance (which will not be used), and a desired level (which will approximate the distance). The output will be the S2 cell's parent at the specified level
// When used to query, we will apply this function to all S2 cells that contains a sample, and return only those with the same parent as the original input S2 cell.

function getParentCell(inputCell, distance, level) {
    if (level < 0 || level > 30 || !Number.isInteger(level)){
        throw new Error("The S2 level must be an integer value in the range [0, 30].");
    }
    else {
        if (level < 30){
            var parentCell = inputCell.slice(0, 3 + 2 * level) + "1";
        }
        else {
            var parentCell = inputCell.slice(0, 3 + 2 * level);
        }
        
        parentCell = parentCell.padEnd(64, "0");
        return parentCell;
    }
}

// Example usage
const input = "0100011001001010001111111000101001100100101010000101000000000000";
const goal = "0100011001001010001111110000000000000000000000000000000000000000"
const distance = 8000; // in meters
const level = 10;
const parentCell = getParentCell(input, distance, level);
console.log(parentCell == goal);
