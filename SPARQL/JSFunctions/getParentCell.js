const nodes2ts = require('nodes2ts');

/**
 * Returns the parent S2 cell ID of a specified S2 cell ID at a given level.
 * @param {string} inputCell - The S2 cell ID.
 * @param {number} level - The S2 level of the parent cell.
 * @returns {string} The parent S2 cell ID.
 */
function getParentCell(inputCell, level) {
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
const level = 10;
const parentCell = getParentCell(input, level);
console.log(parentCell == goal);
