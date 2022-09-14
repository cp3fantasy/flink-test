var params = {
  fullscreen: true
  // domElement: document.getElementById("plan")
};

const canvas = document.getElementById('canvas');
canvas.width = 2000;
canvas.height = 2000;
// canvas.scrollWidth=2000;
// canvas.scrollHeight=2000;
const ctx = canvas.getContext('2d');

const leftMargin = 100;
const topMargin = 100;

const boxWidth = 150;
const boxHeight = 150;

const margin = 50;

const textWith = 140;

// ctx.fillRect(10, 10, 150, 200);

const vertices = json.vertices;
const plan = json.plan;
const nodes = plan.nodes;
// console.log(plan.nodes);

const verticeMap = new Map();
vertices.forEach(vertice => verticeMap.set(vertice.id, vertice));
const nodeMap = new Map();
nodes.forEach(node => {
  nodeMap.set(node.id, node);
});
nodes.forEach(node => {
  if (node.inputs) {
    node.inputs.forEach(input => {
      var inputNode = nodeMap.get(input.id);
      if (!inputNode.outputs) {
        inputNode.outputs = [];
      }
      inputNode.outputs.push(node);
    });
  }
});

// nodes.forEach(node => {
//   console.log(node.outputs);
// });

function calculateLayers() {
  const ids = new Set();
  nodes.forEach(node => {
    ids.add(node.id);
  });
  const layers = [];
  var xIndex = 0;
  while (ids.size > 0) {
    // console.log("ids:" + ids.size);
    var layer = [];
    ids.forEach(id => {
      var node = nodeMap.get(id);
      if (!node.inputs) {
        node.inputs = [];
      }
      var ready = true;
      node.inputs.forEach(input => {
        var inputNode = nodeMap.get(input.id);
        if (!inputNode.hasOwnProperty('xIndex')) {
          ready = false;
        }
      });
      if (ready) {
        node.xIndex = xIndex;
        node.yIndex = layer.length;
        layer.push(node);
      }
    });
    layers.push(layer);
    // console.log(layers);
    layer.forEach(node => {
      ids.delete(node.id);
    })
    xIndex++;
  }
  return layers;
}

function getLines(ctx, text, maxWidth) {
  // var words = text.split(" ");
  var lines = [];
  var currentLine = text[0];

  for (var i = 1; i < text.length; i++) {
    var c = text[i];
    var width = ctx.measureText(currentLine + c).width;
    if (width < maxWidth) {
      currentLine += c;
    } else {
      lines.push(currentLine);
      currentLine = c;
    }
  }
  lines.push(currentLine);
  return lines;
}

// function findWidestLayerIndex(layers) {
//   var index = 0;
//   var width = 0;
//   for (var i = 0; i < layers.length; i++) {
//     if (layers[i].length > width) {
//       index = i;
//       width = layers[i].length;
//     }
//   }
//   return index;
// }

const layers = calculateLayers();
console.log(layers.length);

function getRoot() {
  for (var i = 0; i < layers.length; i++) {
    if (layers[i].length == 1) {
      return layers[i][0];
    }
  }
  return null;
}

function setTreeYIndexLeft(parentIndex, childIndex) {
  var parentLayer = layers[parentIndex];
  var childLayer = layers[childIndex];
  parentLayer.forEach(node => {
    node.inputs.forEach(input => {
      var inputNode = nodeMap.get(input.id);
      if (!inputNode.hasOwnProperty('xIndex')) {
        ready = false;
      }
    });
  });
}

function buildLeftTree(rootNode) {
  var root = { node: rootNode, children: [], height: 1, width: 1, size: 1 };
  if (rootNode.inputs) {
    var width = 0;
    var height = 0;
    rootNode.inputs.forEach(input => {
      var inputNode = nodeMap.get(input.id);
      if (inputNode.outputs.length == 1) {
        var child = buildLeftTree(inputNode);
        root.children.push(child);
        root.size += child.size;
        width += child.width;
        if (child.height > height) {
          height = child.height;
        }
      }
    });
    root.height += height;
    if (width > 1) {
      root.width = width;
    }
  }
  return root;
}

function buildRightTree(rootNode) {
  var root = { node: rootNode, children: [], height: 1, width: 1, size: 1 };
  if (rootNode.outputs) {
    var width = 0;
    var height = 0;
    rootNode.outputs.forEach(output => {
      if (output.inputs.length == 1) {
        var child = buildRightTree(output);
        root.children.push(child);
        root.size += child.size;
        width += child.width;
        if (child.height > height) {
          height = child.height;
        }
      }
    });
    root.height += height;
    if (width > 1) {
      root.width = width;
    }
  }
  return root;
}

function setLeftTreeLayout(root) {
  if (root.children) {
    var yIndex = root.node.yIndex + 0.5 - root.width / 2;
    root.children.forEach(child => {
      child.node.xIndex = root.node.xIndex - 1;
      child.node.yIndex = yIndex + (child.width - 1) / 2;
      yIndex += child.width;
      setLeftTreeLayout(child);
    });
  }
}

function setRightTreeLayout(root) {
  if (root.children) {
    var yIndex = root.node.yIndex + 0.5 - root.width / 2;
    root.children.forEach(child => {
      child.node.xIndex = root.node.xIndex + 1;
      child.node.yIndex = yIndex + (child.width - 1) / 2;
      yIndex += child.width;
      setRightTreeLayout(child);
    });
  }
}

function setTreeLayout(leftRoot, rightRoot) {
  var rootNode = leftRoot.node;
  rootNode.xIndex = leftRoot.height - 1;
  rootNode.yIndex = ((leftRoot.width >= rightRoot.width ? leftRoot.width : rightRoot.width) - 1) / 2;
  console.log("root yIndex:" + rootNode.yIndex);
  setLeftTreeLayout(leftRoot);
  setRightTreeLayout(rightRoot);
}

function buildTrees() {
  var rootNode = getRoot();
  if (!rootNode) {
    return false;
  }
  var leftRoot = buildLeftTree(rootNode);
  console.log(leftRoot);
  var rightRoot = buildRightTree(rootNode);
  console.log(rightRoot);
  if (leftRoot.size + rightRoot.size - 1 == nodes.length) {
    setTreeLayout(leftRoot, rightRoot);
    return true;
  }
}

if (buildTrees()) {
  console.log("build trees ok");
  // drawWithTreeLayout(layers);
}

// var widestLayerIndex = findWidestLayerIndex(layers);


// for (var i = 0; i < layers.length; i++) {
//   // console.log("layer:"+i);
//   var x = leftMargin + i * (boxWidth + margin);
//   var layer = layers[i];
//   for (var j = 0; j < layer.length; j++) {
//     var node = layer[j];
//     var vertice = verticeMap.get(node.id);
//     var y = topMargin + j * (boxHeight + margin);
//     ctx.fillStyle = 'rgba(0, 0, 200, 0.2)';
//     // console.log(x + "," + y);
//     ctx.fillRect(x, y, boxWidth, boxHeight);
//     var lines = getLines(ctx, vertice.name, textWith);
//     ctx.font = '10px serif';
//     ctx.fillStyle = "black";
//     for (var n = 0; n < lines.length; n++) {
//       ctx.fillText(lines[n], x + 5, y + 20 + n * 10);
//     }
//   }
// }



nodes.forEach(node => {
  var vertice = verticeMap.get(node.id);
  var x = leftMargin + node.xIndex * (boxWidth + margin);
  var y = topMargin + node.yIndex * (boxHeight + margin);
  ctx.fillStyle = 'rgba(0, 0, 250, 0.4)';
  // console.log(x + "," + y);
  ctx.fillRect(x, y, boxWidth, boxHeight);
  var lines = getLines(ctx, vertice.name, textWith);
  ctx.font = '10px';
  ctx.fillStyle = "black";
  for (var n = 0; n < lines.length; n++) {
    ctx.fillText(lines[n], x + 5, y + 20 + n * 10);
  }
  node.inputs.forEach(input => {
    var inputNode = nodeMap.get(input.id);
    var sx = leftMargin + inputNode.xIndex * (boxWidth + margin) + boxWidth;
    var sy = topMargin + inputNode.yIndex * (boxHeight + margin) + boxHeight / 2;
    var ex = sx + margin;
    var ey = topMargin + node.yIndex * (boxHeight + margin) + boxHeight / 2;
    console.log(sx + "," + sy + "-->" + ex + "," + ey)
    ctx.beginPath();
    ctx.moveTo(sx, sy);
    ctx.lineTo(ex, ey);
    ctx.stroke();
  });
});






