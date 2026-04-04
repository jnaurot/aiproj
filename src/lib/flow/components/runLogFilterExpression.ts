type TokenKind = 'term' | 'and' | 'or' | 'not' | 'lparen' | 'rparen' | 'eof';

type Token = {
	kind: TokenKind;
	value?: string;
};

type ExprNode =
	| { type: 'term'; value: string }
	| { type: 'not'; value: ExprNode }
	| { type: 'and'; left: ExprNode; right: ExprNode }
	| { type: 'or'; left: ExprNode; right: ExprNode };

function isWhitespace(ch: string): boolean {
	return /\s/.test(ch);
}

function isPrimaryStart(kind: TokenKind): boolean {
	return kind === 'term' || kind === 'lparen' || kind === 'not';
}

function tokenize(input: string): Token[] {
	const src = String(input ?? '');
	const tokens: Token[] = [];
	let i = 0;

	const pushTerm = (value: string) => {
		const term = value.trim();
		if (term.length > 0) tokens.push({ kind: 'term', value: term });
	};

	while (i < src.length) {
		const ch = src[i];
		if (isWhitespace(ch)) {
			i += 1;
			continue;
		}
		if (ch === '&') {
			tokens.push({ kind: 'and' });
			i += 1;
			continue;
		}
		if (ch === '|') {
			tokens.push({ kind: 'or' });
			i += 1;
			continue;
		}
		if (ch === '!') {
			tokens.push({ kind: 'not' });
			i += 1;
			continue;
		}
		if (ch === '(' || ch === '[') {
			tokens.push({ kind: 'lparen' });
			i += 1;
			continue;
		}
		if (ch === ')' || ch === ']') {
			tokens.push({ kind: 'rparen' });
			i += 1;
			continue;
		}
		if (ch === '"' || ch === "'") {
			const quote = ch;
			i += 1;
			let value = '';
			while (i < src.length) {
				const c = src[i];
				if (c === '\\' && i + 1 < src.length) {
					value += src[i + 1];
					i += 2;
					continue;
				}
				if (c === quote) {
					i += 1;
					break;
				}
				value += c;
				i += 1;
			}
			pushTerm(value);
			continue;
		}
		let value = '';
		while (i < src.length) {
			const c = src[i];
			if (c === '\\' && i + 1 < src.length) {
				value += src[i + 1];
				i += 2;
				continue;
			}
			if (
				isWhitespace(c) ||
				c === '&' ||
				c === '|' ||
				c === '(' ||
				c === ')' ||
				c === '[' ||
				c === ']'
			) {
				break;
			}
			value += c;
			i += 1;
		}
		pushTerm(value);
	}
	tokens.push({ kind: 'eof' });
	return tokens;
}

class Parser {
	private readonly tokens: Token[];
	private index = 0;

	constructor(tokens: Token[]) {
		this.tokens = tokens;
	}

	private current(): Token {
		return this.tokens[this.index] ?? { kind: 'eof' };
	}

	private consume(kind: TokenKind): boolean {
		if (this.current().kind !== kind) return false;
		this.index += 1;
		return true;
	}

	private expect(kind: TokenKind): void {
		if (!this.consume(kind)) {
			throw new Error(`Expected ${kind}`);
		}
	}

	parse(): ExprNode | null {
		if (this.current().kind === 'eof') return null;
		const expr = this.parseOr();
		this.expect('eof');
		return expr;
	}

	private parseOr(): ExprNode {
		let left = this.parseAnd();
		while (this.consume('or')) {
			const right = this.parseAnd();
			left = { type: 'or', left, right };
		}
		return left;
	}

	private parseAnd(): ExprNode {
		let left = this.parseUnary();
		while (true) {
			if (this.consume('and')) {
				const right = this.parseUnary();
				left = { type: 'and', left, right };
				continue;
			}
			if (isPrimaryStart(this.current().kind)) {
				const right = this.parseUnary();
				left = { type: 'and', left, right };
				continue;
			}
			break;
		}
		return left;
	}

	private parseUnary(): ExprNode {
		if (this.consume('not')) {
			return { type: 'not', value: this.parseUnary() };
		}
		return this.parsePrimary();
	}

	private parsePrimary(): ExprNode {
		const token = this.current();
		if (this.consume('term')) {
			return { type: 'term', value: String(token.value ?? '') };
		}
		if (this.consume('lparen')) {
			const expr = this.parseOr();
			this.expect('rparen');
			return expr;
		}
		throw new Error(`Unexpected token ${token.kind}`);
	}
}

function evaluate(node: ExprNode, haystack: string): boolean {
	if (node.type === 'term') return haystack.includes(node.value.toLowerCase());
	if (node.type === 'not') return !evaluate(node.value, haystack);
	if (node.type === 'and') return evaluate(node.left, haystack) && evaluate(node.right, haystack);
	return evaluate(node.left, haystack) || evaluate(node.right, haystack);
}

export function buildRunLogFilterPredicate(filter: string): (text: string) => boolean {
	const raw = String(filter ?? '').trim();
	if (!raw) return () => true;
	const fallbackTerms = tokenize(raw)
		.filter((token) => token.kind === 'term')
		.map((token) => String(token.value ?? '').toLowerCase())
		.filter((value) => value.length > 0);
	const hayNeedle = raw.toLowerCase();
	try {
		const ast = new Parser(tokenize(raw)).parse();
		if (!ast) return () => true;
		return (text: string) => evaluate(ast, String(text ?? '').toLowerCase());
	} catch {
		// Graceful fallback for malformed expressions.
		if (fallbackTerms.length > 0) {
			return (text: string) => {
				const hay = String(text ?? '').toLowerCase();
				return fallbackTerms.some((term) => hay.includes(term));
			};
		}
		return (text: string) => String(text ?? '').toLowerCase().includes(hayNeedle);
	}
}
