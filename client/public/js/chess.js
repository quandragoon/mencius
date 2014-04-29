// Wrapper around ChessBoard object to manage game logic
// All input will be done via chatroom messages using
// board.move() function
function ChessManager() {

	// Create non draggable board
	var board = new ChessBoard('board1', {} );

	// Store full FEN information
	var turn = 'w';
	var castle = 'KQkq';
	var enp = '-';
	var clock = '0';	// halfmove clock for fifty-move rule
	var moveNumber = '1';		// move counter, increments on black move
	
	var pos = board.position();	// variable to access current board position

	// Performs move on ChessBoard model
	// assumes move is valid
	this.performMove = function(move) {
		if (this.checkMove(move) === false) return;

		var tmp = move.split('-');
		
		clock++;
		if (turn === 'b') moveNumber++;	// Increment move number on black move

		// Perform castling
		if (tmp[0] === '0') {
			
			if (tmp.length === 2) { // Kingside
				if (turn === 'w') {
					board.move('e1-g1');
					board.move('h1-f1');
				} else {
					board.move('e8-g8');
					board.move('h8-f8');
				}
			}

			if (tmp.length === 3) {	// Queenside
				if (turn === 'w') {
					board.move('e1-c1');
					board.move('a1-d1');
				} else {
					board.move('e8-c8');
					board.move('a8-d8');
				}
			}

			// disable castling
			var toRemove = 'kq';
			if (turn === 'w') toRemove = 'KQ';
			castle = castle.replace(toRemove, '');

			enp = '-';

			turn = (turn === 'w')?'b':'w';
			return;
		}

		var from = tmp[0];
		var to = tmp[1];
		
		var piece = pos[from][1].toLowerCase();
		switch(piece) {
		case 'p':	// pawn
			clock = 0;

			// Remove captured piece in en passant
			if (from[0] !== from[1]) {
				if (to === enp) {
					var enpMove = from + '-' + to[0] + from[1] 
					board.move(enpMove);
					board.move(to[0] + from[1] + '-' + to);
					enp = '-';
					turn = (turn === 'w')?'b':'w';
					return;
				}
			}

			// Update en passant if distance is two
			var fromY = parseInt(from[1]);
			var toY = parseInt(to[1]);
			if (Math.abs(toY - fromY) === 2) {
				enp = from[0] + (fromY + (turn==='w'?1:-1));
			}

			break;
		case 'r':	// rook
			if (from === 'a1') castle = castle.replace('Q','');
			if (from === 'h1') castle = castle.replace('K','');
			if (from === 'a8') castle = castle.replace('q','');
			if (from === 'h8') castle = castle.replace('k','');
			enp = '-';
			break;
		case 'k':	// king
			// King movement disables castling for current color
			if (turn === 'w') { 
				castle = castle.replace('K', '');
				castle = castle.replace('Q', '');
			} else {
				castle = castle.replace('k', '');
				castle = castle.replace('q', '');
			}
		case 'n':	// knight
		case 'b':	// bishop
		case 'q':	// queen
			enp = '-';
			break;
		default:
			alert("unrecognized piece: " + piece);
		}

		turn = (turn === 'w')?'b':'w';
		board.move(move);
	};
	
	// Checks if proposed move is valid
	// apply is boolean. If true, move will update game state.
	this.checkMove = function(move) {
		// Update position
		pos = board.position();

		// Check syntax of move
		if (this.validMoveSyntax(move) !== true) return false;

		// Check game logic validity
		if (this.validMoveLogic(move) !== true) return false;

		return true;		
	};

	// Taken from chessboard-0.3.0.js with additional castling check
	this.validMoveSyntax = function(move) {
		// move should be a string
		if (typeof move !== 'string') return false;

		// move should be in the form of "e2-e4", "f6-d5"
		// adding support for "0-0", "0-0-0"
		var tmp = move.split('-');
		if (tmp.length === 2) {
			if (tmp[0] === '0' && tmp[0] === tmp[1]) return true;
			return (this.validSquare(tmp[0]) === true && this.validSquare(tmp[1]) === true);
		}
		if (tmp.length === 3) {
			// Only accept "0-0-0" for moves of length 3
			if (tmp[0] === '0' && tmp[0] === tmp[1] && tmp[1] === tmp[2])
				return true;
		}
		return false;		
	};

	// Taken from chessboard-0.3.0.js
	this.validSquare = function(square) {
		if (typeof square !== 'string') return false;
		return (square.search(/^[a-h][1-8]$/) !== -1);
	};

	// Checks if a move adheres to game logic rules
	this.validMoveLogic = function(move) {
		var tmp = move.split('-');
		
		// Check castling
		if (tmp[0] === '0') return this.checkCastle(tmp.length);

		var fromSquare = tmp[0];
		var toSquare = tmp[1];

		// Check piece exists
		if (pos[fromSquare] === undefined) return false;

		// Check piece belongs to current player
		if (pos[fromSquare][0] !== turn) return false;

		// Check piece movement
		if (this.checkPieceMovement(fromSquare, toSquare) !== true) return false;

		// Check final square is not occupied by own team
		if (pos[toSquare] != undefined && pos[toSquare][0] === turn) return false;

		// Check if king will be in check if piece is moved
		
		// project move and check if king is in danger
		var nextPos = board.position();
		nextPos[tmp[1]] = nextPos[tmp[0]];
		delete nextPos[tmp[0]];
		if (this.kingInCheck(nextPos, turn)) {
			alert("check");
			return false;
		}
		return true;
	}


	this.indexToPos = function(x, y) {
		return 'abcdefgh'[x] + '12345678'[y];
	}

	this.posToIndex = function(p) {
		return ['abcdefgh'.indexOf(p[0]), '12345678'.indexOf(p[1])];
	}

	// Checks given position if white or black king is in check
	this.kingInCheck = function(position, whiteOrBlack) {
		var king = (whiteOrBlack == 'w') ? 'wK' : 'bK';
		var kingPos = '';

		var nPos = [];
		var kPos = '';
		// First find king position
		for (var p in position) {
			if (position[p][0] === whiteOrBlack) {
				if (position[p][1] == 'K') {
					kingPos = p;
				}
				continue;
			}	
			if (position[p][1] == 'N') {
				nPos.push(p);
			}
			if (position[p][1] == 'K') {
				kPos = p;
			}
		}
		var kingInd = this.posToIndex(kingPos);
		// Start from king position, check if can be attacked by any piece

		// King adjacent
		var xOffset = kingPos.charCodeAt(0) - kPos.charCodeAt(0);
		var yOffset = kingPos.charCodeAt(1) - kPos.charCodeAt(1);
		
		if (Math.abs(xOffset) < 2 && Math.abs(yOffset) < 2) return true;

		// Knights
		for (var i = 0; i < nPos.length; i++) {
			nInd = this.posToIndex(nPos);
			var xOffset = nInd[0] - kingInd[0];
			var yOffset = nInd[1] - kingInd[1];
			if ((xOffset * yOffset !== 0) && (Math.abs(xOffset) + Math.abs(yOffset)) === 3) {
				return true;
			}
		}

		// Rooks / Queens along row+col
		var offsets = [[0,1],[1,0],[0,-1],[-1,0]];
		for (var i = 0; i < offsets.length; i++) {
			var off = offsets[i];
			var x = kingInd[0] + off[0];
			var y = kingInd[1] + off[1];
			while (x >= 0 && x < 8 && y >= 0 && y < 8) {
				var p = this.indexToPos(x, y);
				x += off[0];
				y += off[1];
				if (position[p] === undefined) continue;
				if (position[p][0] === whiteOrBlack) break;	// Friendly piece
				if ('RQ'.indexOf(position[p][1]) !== -1) return true;
			}
		}	

		// Bishops / Queens
		offsets = [[1,1],[1,-1],[-1,-1],[-1,1]];
		for (var i = 0; i < offsets.length; i++) {
			var off = offsets[i];
			var x = kingInd[0] + off[0];
			var y = kingInd[1] + off[1];
			while (x >= 0 && x < 8 && y >= 0 && y < 8) {
				var p = this.indexToPos(x, y);
				x += off[0];
				y += off[1];
				if (position[p] === undefined) continue;
				if (position[p][0] === whiteOrBlack) break;	// Friendly piece
				if ('BQ'.indexOf(position[p][1]) !== -1) return true;
			}
		}	

		// Pawns
		var offset = 1;
		if (whiteOrBlack === 'b') {
			offset = -1;
		}	
		var x = kingInd[0] + 1;
		var y = kingInd[1] + offset;
		if (x >= 0 && x < 8 && y >= 0 && y < 8) {
			var p = this.indexToPos(x, y);
			if (position[p] !== undefined && position[p][0] !== whiteOrBlack && position[p][1] === 'P') return true;
		}
		x = kingInd[0] - 1;
		if (x >= 0 && x < 8 && y >= 0 && y < 8) {
			var p = this.indexToPos(x, y);
			if (position[p] !== undefined && position[p][0] !== whiteOrBlack && position[p][1] === 'P') return true;
		}

		return false;
	}

	// Checks if castling is valid.
	// distance = 3 : queenside
	// distance = 2 : kingside
	this.checkCastle = function(distance) {
		var notation = 'K';
		if (turn === 'b') {
			if (distance === 2){
				notation = 'k';
			}
			else if (distance === 3) {
				notation = 'q'
			}
		} else if (distance === 3) {
			notation = 'Q'
		}

		// Check if castling still valid
		if (castle.search(notation) === -1) return false;

		// Check if pieces in the way
		if (notation === 'K')	// White kingside
			return (pos['f1'] === undefined && pos['g1'] === undefined);
		if (notation === 'Q')	// White queenside
			return (pos['b1'] === undefined && pos['c1'] === undefined && pos['d1'] === undefined);
		if (notation === 'k')	// Black kingside
			return (pos['f8'] === undefined && pos['g8'] === undefined);
		if (notation === 'q')	// Black queenside
			return (pos['b8'] === undefined && pos['c8'] === undefined && pos['d8'] === undefined);

		alert("should not be here in checkCastle");
		// Should not get here
		return true;
	};

	// Check that the move follows piece movement rules
	this.checkPieceMovement = function(from, to) {
		// Require an actual move
		if (from === to) return false;

		var piece = pos[from][1].toLowerCase();
		switch(piece) {
		case 'p':	// pawn
			return this.checkPawnMovement(from, to);
			break;
		case 'r':	// rook
			return this.checkRookMovement(from, to);
			break;
		case 'n':	// knight
			return this.checkKnightMovement(from, to);
			break;
		case 'b':	// bishop
			return this.checkBishopMovement(from, to);
			break;
		case 'q':	// queen
			return this.checkQueenMovement(from, to);
			break;
		case 'k':	// king
			return this.checkKingMovement(from, to);
			break;
		default:
			alert("unrecognized piece: " + piece);
		}
	};

	// Pawn has 4 movement options
	// forward one, forward two, en passant, diagonal capture
	this.checkPawnMovement = function(from, to) {
		var xOffset = from.charCodeAt(0) - to.charCodeAt(0);
		var fromY = parseInt(from[1]);
		var toY = parseInt(to[1]);

		var direction = (turn === 'w') ? 1 : -1;
		// Check forward movement
		if (xOffset === 0) {
			if (Math.abs(toY - fromY) > 2) return false;

			// Cannot capture forward, must check square in front 
			var forwardSquare = from[0] + (fromY + direction);
			if (pos[forwardSquare] !== undefined) return false;	//Square occupied
			if (toY - fromY === direction) return true;	// Valid forward
			
			// Double forward movement only possible if on start position
			// on row 2 for white, row 7 for black
			if (turn === 'w' && fromY !== 2) return false;
			if (turn === 'b' && fromY !== 7) return false;

			// Check next square is unoccupied
			forwardSquare = from[0] + (fromY + 2*direction);
			if (pos[forwardSquare] === undefined) return true;	//Square open

			// No other valid forward distance moves
			return false;
		}
		// Now check diagonal movement
		if (Math.abs(xOffset) === 1) {
			if (Math.abs(toY - fromY) !== 1) return false;

			// Check if piece to take
			if (pos[to] !== undefined) return true;

			// En passant check
			return (to === enp);
		}

		// Other movements invalid
		return false;
	};

	// Rook must be in same column or row
	this.checkRookMovement = function(from, to) {
		if (from[0] !== to[0] && from[1] !== to[1]) return false;

		// Check that no pieces in the way
		var fromInd = this.posToIndex(from);
		var toInd = this.posToIndex(to);
		
		// Same column
		if (fromInd[0] === toInd[0]) {
			var dir = (toInd[1] - fromInd[1]) / Math.abs(toInd[1] - fromInd[1]);
			var a = fromInd[1] + dir;
			while (a !== toInd[1]) {
				var p = this.indexToPos(fromInd[0], a);
				if (pos[p] !== undefined) return false;
				a += dir;
			}
		}

		// Same row
		if (fromInd[1] === toInd[1]) {
			var dir = (toInd[0] - fromInd[0]) / Math.abs(toInd[0] - fromInd[0]);
			var b = fromInd[0] + dir;
			while (b !== toInd[0]) {
				var p = this.indexToPos(b, fromInd[1]);
				if (pos[p] !== undefined) return false;
				b += dir;
			}
		}

		return true;
	};

	// Knight moves in an L shape
	this.checkKnightMovement = function(from, to) {
		// Valid offsets are
		//  [1,2, 2,1, 2,-1, 1,-2, -1,-2, -2,-1, -2,1, -1,2];

		var xOffset = from.charCodeAt(0) - to.charCodeAt(0);
		var yOffset = from.charCodeAt(1) - to.charCodeAt(1);
		
		return ((xOffset * yOffset !== 0) && (Math.abs(xOffset) + Math.abs(yOffset)) === 3);
	};

	// Bishop moves in a diagonal
	this.checkBishopMovement = function(from, to) {
		// Valid offsets are row +/- n, col +/- n

		var xOffset = to.charCodeAt(0) - from.charCodeAt(0);
		var yOffset = to.charCodeAt(1) - from.charCodeAt(1);
		
		if (Math.abs(xOffset) !== Math.abs(yOffset)) return false;

		// Check that no pieces in the way
		var fromInd = this.posToIndex(from);
		var toInd = this.posToIndex(to);

		var offset = [xOffset/Math.abs(xOffset), yOffset/Math.abs(yOffset)];
		var x = fromInd[0] + offset[0];
		var y = fromInd[1] + offset[1];

		while (x !== toInd[0] || y !== toInd[1]) {
			var p = this.indexToPos(x, y);
			if (pos[p] !== undefined) return false;
			x += offset[0];
			y += offset[1];
		}

		return true;
	};

	// Queen moves like rook or bishop
	this.checkQueenMovement = function(from, to) {
		return (this.checkRookMovement(from, to) 
				|| this.checkBishopMovement(from, to));
	};

	// King moves any direction with distance one
	this.checkKingMovement = function(from, to) {
		var xOffset = from.charCodeAt(0) - to.charCodeAt(0);
		var yOffset = from.charCodeAt(1) - to.charCodeAt(1);
		
		return (Math.abs(xOffset) < 2 && Math.abs(yOffset) < 2);
	};

	this.getChessBoard = function() {
		return board;
	};

	this.startGame = function() {
		board.start();
	};
}


var board1 = new ChessManager();
board1.startGame();
