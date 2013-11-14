package cn.ac.iie.cls.etl.dataprocess.operator.recordoperator;
// $ANTLR 3.4 G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g 2013-08-21 11:37:12

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

/**
	@auther :	hanbing
	date	:	2013-08-09
**/
@SuppressWarnings({"all", "warnings", "unchecked"})
public class FilterLexer extends Lexer {
    public static final int EOF=-1;
    public static final int A=4;
    public static final int B=5;
    public static final int C=6;
    public static final int D=7;
    public static final int DOT=8;
    public static final int DOUBLEQUOTED_STRING=9;
    public static final int Digit=10;
    public static final int E=11;
    public static final int EQ=12;
    public static final int ESC=13;
    public static final int EXPONENT=14;
    public static final int F=15;
    public static final int FLOAT=16;
    public static final int G=17;
    public static final int GEQ=18;
    public static final int GTH=19;
    public static final int H=20;
    public static final int I=21;
    public static final int ID=22;
    public static final int INT=23;
    public static final int J=24;
    public static final int K=25;
    public static final int KW_AND=26;
    public static final int KW_BETWEEN=27;
    public static final int KW_FALSE=28;
    public static final int KW_LIKE=29;
    public static final int KW_NOT=30;
    public static final int KW_NULL=31;
    public static final int KW_OR=32;
    public static final int KW_TRUE=33;
    public static final int L=34;
    public static final int LEQ=35;
    public static final int LPAREN=36;
    public static final int LTH=37;
    public static final int Letter=38;
    public static final int M=39;
    public static final int MOD=40;
    public static final int N=41;
    public static final int NOT_EQ=42;
    public static final int O=43;
    public static final int P=44;
    public static final int Q=45;
    public static final int QUOTED_STRING=46;
    public static final int R=47;
    public static final int RPAREN=48;
    public static final int S=49;
    public static final int T=50;
    public static final int U=51;
    public static final int V=52;
    public static final int W=53;
    public static final int WS=54;
    public static final int X=55;
    public static final int Y=56;
    public static final int Z=57;

    // delegates
    // delegators
    public Lexer[] getDelegates() {
        return new Lexer[] {};
    }

    public FilterLexer() {} 
    public FilterLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public FilterLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);
    }
    public String getGrammarFileName() { return "G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g"; }

    // $ANTLR start "KW_AND"
    public final void mKW_AND() throws RecognitionException {
        try {
            int _type = KW_AND;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:10:9: ( A N D )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:10:11: A N D
            {
            mA(); 


            mN(); 


            mD(); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_AND"

    // $ANTLR start "KW_LIKE"
    public final void mKW_LIKE() throws RecognitionException {
        try {
            int _type = KW_LIKE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:11:10: ( L I K E )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:11:12: L I K E
            {
            mL(); 


            mI(); 


            mK(); 


            mE(); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_LIKE"

    // $ANTLR start "KW_NOT"
    public final void mKW_NOT() throws RecognitionException {
        try {
            int _type = KW_NOT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:12:9: ( N O T )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:12:11: N O T
            {
            mN(); 


            mO(); 


            mT(); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_NOT"

    // $ANTLR start "KW_OR"
    public final void mKW_OR() throws RecognitionException {
        try {
            int _type = KW_OR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:13:8: ( O R )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:13:10: O R
            {
            mO(); 


            mR(); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_OR"

    // $ANTLR start "KW_NULL"
    public final void mKW_NULL() throws RecognitionException {
        try {
            int _type = KW_NULL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:14:10: ( N U L L )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:14:12: N U L L
            {
            mN(); 


            mU(); 


            mL(); 


            mL(); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_NULL"

    // $ANTLR start "KW_TRUE"
    public final void mKW_TRUE() throws RecognitionException {
        try {
            int _type = KW_TRUE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:15:10: ( T R U E )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:15:12: T R U E
            {
            mT(); 


            mR(); 


            mU(); 


            mE(); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_TRUE"

    // $ANTLR start "KW_FALSE"
    public final void mKW_FALSE() throws RecognitionException {
        try {
            int _type = KW_FALSE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:16:10: ( F A L S E )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:16:12: F A L S E
            {
            mF(); 


            mA(); 


            mL(); 


            mS(); 


            mE(); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_FALSE"

    // $ANTLR start "KW_BETWEEN"
    public final void mKW_BETWEEN() throws RecognitionException {
        try {
            int _type = KW_BETWEEN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:17:12: ( B E T W E E N )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:17:14: B E T W E E N
            {
            mB(); 


            mE(); 


            mT(); 


            mW(); 


            mE(); 


            mE(); 


            mN(); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_BETWEEN"

    // $ANTLR start "LPAREN"
    public final void mLPAREN() throws RecognitionException {
        try {
            int _type = LPAREN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:21:10: ( '(' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:21:13: '('
            {
            match('('); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "LPAREN"

    // $ANTLR start "RPAREN"
    public final void mRPAREN() throws RecognitionException {
        try {
            int _type = RPAREN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:22:10: ( ')' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:22:13: ')'
            {
            match(')'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "RPAREN"

    // $ANTLR start "EQ"
    public final void mEQ() throws RecognitionException {
        try {
            int _type = EQ;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:23:6: ( '=' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:23:9: '='
            {
            match('='); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "EQ"

    // $ANTLR start "LEQ"
    public final void mLEQ() throws RecognitionException {
        try {
            int _type = LEQ;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:24:7: ( '<=' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:24:10: '<='
            {
            match("<="); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "LEQ"

    // $ANTLR start "LTH"
    public final void mLTH() throws RecognitionException {
        try {
            int _type = LTH;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:25:7: ( '<' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:25:10: '<'
            {
            match('<'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "LTH"

    // $ANTLR start "GEQ"
    public final void mGEQ() throws RecognitionException {
        try {
            int _type = GEQ;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:26:7: ( '>=' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:26:10: '>='
            {
            match(">="); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "GEQ"

    // $ANTLR start "GTH"
    public final void mGTH() throws RecognitionException {
        try {
            int _type = GTH;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:27:7: ( '>' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:27:10: '>'
            {
            match('>'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "GTH"

    // $ANTLR start "NOT_EQ"
    public final void mNOT_EQ() throws RecognitionException {
        try {
            int _type = NOT_EQ;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:28:9: ( '!=' | '<>' | '^=' )
            int alt1=3;
            switch ( input.LA(1) ) {
            case '!':
                {
                alt1=1;
                }
                break;
            case '<':
                {
                alt1=2;
                }
                break;
            case '^':
                {
                alt1=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 1, 0, input);

                throw nvae;

            }

            switch (alt1) {
                case 1 :
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:28:12: '!='
                    {
                    match("!="); 



                    }
                    break;
                case 2 :
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:28:19: '<>'
                    {
                    match("<>"); 



                    }
                    break;
                case 3 :
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:28:26: '^='
                    {
                    match("^="); 



                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "NOT_EQ"

    // $ANTLR start "MOD"
    public final void mMOD() throws RecognitionException {
        try {
            int _type = MOD;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:29:7: ( '%' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:29:10: '%'
            {
            match('%'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "MOD"

    // $ANTLR start "DOT"
    public final void mDOT() throws RecognitionException {
        try {
            int _type = DOT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:30:7: ( '.' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:30:10: '.'
            {
            match('.'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "DOT"

    // $ANTLR start "ID"
    public final void mID() throws RecognitionException {
        try {
            int _type = ID;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:33:5: ( Letter ( Letter | Digit | '_' )* )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:33:7: Letter ( Letter | Digit | '_' )*
            {
            mLetter(); 


            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:33:14: ( Letter | Digit | '_' )*
            loop2:
            do {
                int alt2=2;
                int LA2_0 = input.LA(1);

                if ( ((LA2_0 >= '0' && LA2_0 <= '9')||(LA2_0 >= 'A' && LA2_0 <= 'Z')||LA2_0=='_'||(LA2_0 >= 'a' && LA2_0 <= 'z')) ) {
                    alt2=1;
                }


                switch (alt2) {
            	case 1 :
            	    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            	    {
            	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'Z')||input.LA(1)=='_'||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    break loop2;
                }
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "ID"

    // $ANTLR start "QUOTED_STRING"
    public final void mQUOTED_STRING() throws RecognitionException {
        try {
            int _type = QUOTED_STRING;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:35:15: ( ( 'n' )? '\\'' ( '\\'\\'' |~ ( '\\'' ) )* '\\'' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:35:17: ( 'n' )? '\\'' ( '\\'\\'' |~ ( '\\'' ) )* '\\''
            {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:35:17: ( 'n' )?
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0=='n') ) {
                alt3=1;
            }
            switch (alt3) {
                case 1 :
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:35:19: 'n'
                    {
                    match('n'); 

                    }
                    break;

            }


            match('\''); 

            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:35:31: ( '\\'\\'' |~ ( '\\'' ) )*
            loop4:
            do {
                int alt4=3;
                int LA4_0 = input.LA(1);

                if ( (LA4_0=='\'') ) {
                    int LA4_1 = input.LA(2);

                    if ( (LA4_1=='\'') ) {
                        alt4=1;
                    }


                }
                else if ( ((LA4_0 >= '\u0000' && LA4_0 <= '&')||(LA4_0 >= '(' && LA4_0 <= '\uFFFF')) ) {
                    alt4=2;
                }


                switch (alt4) {
            	case 1 :
            	    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:35:33: '\\'\\''
            	    {
            	    match("''"); 



            	    }
            	    break;
            	case 2 :
            	    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:35:42: ~ ( '\\'' )
            	    {
            	    if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '&')||(input.LA(1) >= '(' && input.LA(1) <= '\uFFFF') ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    break loop4;
                }
            } while (true);


            match('\''); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "QUOTED_STRING"

    // $ANTLR start "DOUBLEQUOTED_STRING"
    public final void mDOUBLEQUOTED_STRING() throws RecognitionException {
        try {
            int _type = DOUBLEQUOTED_STRING;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:37:21: ( '\"' (~ ( '\"' ) )* '\"' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:37:23: '\"' (~ ( '\"' ) )* '\"'
            {
            match('\"'); 

            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:37:27: (~ ( '\"' ) )*
            loop5:
            do {
                int alt5=2;
                int LA5_0 = input.LA(1);

                if ( ((LA5_0 >= '\u0000' && LA5_0 <= '!')||(LA5_0 >= '#' && LA5_0 <= '\uFFFF')) ) {
                    alt5=1;
                }


                switch (alt5) {
            	case 1 :
            	    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            	    {
            	    if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '!')||(input.LA(1) >= '#' && input.LA(1) <= '\uFFFF') ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    break loop5;
                }
            } while (true);


            match('\"'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "DOUBLEQUOTED_STRING"

    // $ANTLR start "INT"
    public final void mINT() throws RecognitionException {
        try {
            int _type = INT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:39:5: ( ( '0' .. '9' )+ )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:39:6: ( '0' .. '9' )+
            {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:39:6: ( '0' .. '9' )+
            int cnt6=0;
            loop6:
            do {
                int alt6=2;
                int LA6_0 = input.LA(1);

                if ( ((LA6_0 >= '0' && LA6_0 <= '9')) ) {
                    alt6=1;
                }


                switch (alt6) {
            	case 1 :
            	    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            	    {
            	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    if ( cnt6 >= 1 ) break loop6;
                        EarlyExitException eee =
                            new EarlyExitException(6, input);
                        throw eee;
                }
                cnt6++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "INT"

    // $ANTLR start "FLOAT"
    public final void mFLOAT() throws RecognitionException {
        try {
            int _type = FLOAT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:43:5: ( ( '0' .. '9' )+ '.' ( '0' .. '9' )* ( EXPONENT )? | '.' ( '0' .. '9' )+ ( EXPONENT )? | ( '0' .. '9' )+ EXPONENT )
            int alt13=3;
            alt13 = dfa13.predict(input);
            switch (alt13) {
                case 1 :
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:43:9: ( '0' .. '9' )+ '.' ( '0' .. '9' )* ( EXPONENT )?
                    {
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:43:9: ( '0' .. '9' )+
                    int cnt7=0;
                    loop7:
                    do {
                        int alt7=2;
                        int LA7_0 = input.LA(1);

                        if ( ((LA7_0 >= '0' && LA7_0 <= '9')) ) {
                            alt7=1;
                        }


                        switch (alt7) {
                    	case 1 :
                    	    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
                    	    {
                    	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                    	        input.consume();
                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;
                    	    }


                    	    }
                    	    break;

                    	default :
                    	    if ( cnt7 >= 1 ) break loop7;
                                EarlyExitException eee =
                                    new EarlyExitException(7, input);
                                throw eee;
                        }
                        cnt7++;
                    } while (true);


                    match('.'); 

                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:43:25: ( '0' .. '9' )*
                    loop8:
                    do {
                        int alt8=2;
                        int LA8_0 = input.LA(1);

                        if ( ((LA8_0 >= '0' && LA8_0 <= '9')) ) {
                            alt8=1;
                        }


                        switch (alt8) {
                    	case 1 :
                    	    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
                    	    {
                    	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                    	        input.consume();
                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;
                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop8;
                        }
                    } while (true);


                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:43:37: ( EXPONENT )?
                    int alt9=2;
                    int LA9_0 = input.LA(1);

                    if ( (LA9_0=='E'||LA9_0=='e') ) {
                        alt9=1;
                    }
                    switch (alt9) {
                        case 1 :
                            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:43:37: EXPONENT
                            {
                            mEXPONENT(); 


                            }
                            break;

                    }


                    }
                    break;
                case 2 :
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:44:9: '.' ( '0' .. '9' )+ ( EXPONENT )?
                    {
                    match('.'); 

                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:44:13: ( '0' .. '9' )+
                    int cnt10=0;
                    loop10:
                    do {
                        int alt10=2;
                        int LA10_0 = input.LA(1);

                        if ( ((LA10_0 >= '0' && LA10_0 <= '9')) ) {
                            alt10=1;
                        }


                        switch (alt10) {
                    	case 1 :
                    	    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
                    	    {
                    	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                    	        input.consume();
                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;
                    	    }


                    	    }
                    	    break;

                    	default :
                    	    if ( cnt10 >= 1 ) break loop10;
                                EarlyExitException eee =
                                    new EarlyExitException(10, input);
                                throw eee;
                        }
                        cnt10++;
                    } while (true);


                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:44:25: ( EXPONENT )?
                    int alt11=2;
                    int LA11_0 = input.LA(1);

                    if ( (LA11_0=='E'||LA11_0=='e') ) {
                        alt11=1;
                    }
                    switch (alt11) {
                        case 1 :
                            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:44:25: EXPONENT
                            {
                            mEXPONENT(); 


                            }
                            break;

                    }


                    }
                    break;
                case 3 :
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:45:9: ( '0' .. '9' )+ EXPONENT
                    {
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:45:9: ( '0' .. '9' )+
                    int cnt12=0;
                    loop12:
                    do {
                        int alt12=2;
                        int LA12_0 = input.LA(1);

                        if ( ((LA12_0 >= '0' && LA12_0 <= '9')) ) {
                            alt12=1;
                        }


                        switch (alt12) {
                    	case 1 :
                    	    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
                    	    {
                    	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                    	        input.consume();
                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;
                    	    }


                    	    }
                    	    break;

                    	default :
                    	    if ( cnt12 >= 1 ) break loop12;
                                EarlyExitException eee =
                                    new EarlyExitException(12, input);
                                throw eee;
                        }
                        cnt12++;
                    } while (true);


                    mEXPONENT(); 


                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "FLOAT"

    // $ANTLR start "Letter"
    public final void mLetter() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:48:18: ( 'A' .. 'Z' | 'a' .. 'z' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( (input.LA(1) >= 'A' && input.LA(1) <= 'Z')||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "Letter"

    // $ANTLR start "Digit"
    public final void mDigit() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:50:18: ( '0' .. '9' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "Digit"

    // $ANTLR start "EXPONENT"
    public final void mEXPONENT() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:52:20: ( ( 'e' | 'E' ) ( '+' | '-' )? ( '0' .. '9' )+ )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:52:23: ( 'e' | 'E' ) ( '+' | '-' )? ( '0' .. '9' )+
            {
            if ( input.LA(1)=='E'||input.LA(1)=='e' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:52:33: ( '+' | '-' )?
            int alt14=2;
            int LA14_0 = input.LA(1);

            if ( (LA14_0=='+'||LA14_0=='-') ) {
                alt14=1;
            }
            switch (alt14) {
                case 1 :
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
                    {
                    if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    }
                    break;

            }


            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:52:44: ( '0' .. '9' )+
            int cnt15=0;
            loop15:
            do {
                int alt15=2;
                int LA15_0 = input.LA(1);

                if ( ((LA15_0 >= '0' && LA15_0 <= '9')) ) {
                    alt15=1;
                }


                switch (alt15) {
            	case 1 :
            	    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            	    {
            	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    if ( cnt15 >= 1 ) break loop15;
                        EarlyExitException eee =
                            new EarlyExitException(15, input);
                        throw eee;
                }
                cnt15++;
            } while (true);


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "EXPONENT"

    // $ANTLR start "ESC"
    public final void mESC() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:54:16: ( '\\\\' ( '\"' | '\\'' | '\\\\' ) )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:54:19: '\\\\' ( '\"' | '\\'' | '\\\\' )
            {
            match('\\'); 

            if ( input.LA(1)=='\"'||input.LA(1)=='\''||input.LA(1)=='\\' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "ESC"

    // $ANTLR start "A"
    public final void mA() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:57:2: ( 'A' | 'a' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='A'||input.LA(1)=='a' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "A"

    // $ANTLR start "B"
    public final void mB() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:60:2: ( 'B' | 'b' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='B'||input.LA(1)=='b' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "B"

    // $ANTLR start "C"
    public final void mC() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:63:2: ( 'C' | 'c' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='C'||input.LA(1)=='c' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "C"

    // $ANTLR start "D"
    public final void mD() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:66:2: ( 'D' | 'd' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='D'||input.LA(1)=='d' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "D"

    // $ANTLR start "E"
    public final void mE() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:69:2: ( 'E' | 'e' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='E'||input.LA(1)=='e' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "E"

    // $ANTLR start "F"
    public final void mF() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:72:2: ( 'F' | 'f' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='F'||input.LA(1)=='f' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "F"

    // $ANTLR start "G"
    public final void mG() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:75:2: ( 'G' | 'g' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='G'||input.LA(1)=='g' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "G"

    // $ANTLR start "H"
    public final void mH() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:78:2: ( 'H' | 'h' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='H'||input.LA(1)=='h' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "H"

    // $ANTLR start "I"
    public final void mI() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:81:2: ( 'I' | 'i' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='I'||input.LA(1)=='i' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "I"

    // $ANTLR start "J"
    public final void mJ() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:84:2: ( 'J' | 'j' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='J'||input.LA(1)=='j' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "J"

    // $ANTLR start "K"
    public final void mK() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:87:2: ( 'K' | 'k' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='K'||input.LA(1)=='k' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "K"

    // $ANTLR start "L"
    public final void mL() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:90:2: ( 'L' | 'l' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='L'||input.LA(1)=='l' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "L"

    // $ANTLR start "M"
    public final void mM() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:93:2: ( 'M' | 'm' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='M'||input.LA(1)=='m' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "M"

    // $ANTLR start "N"
    public final void mN() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:96:2: ( 'N' | 'n' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='N'||input.LA(1)=='n' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "N"

    // $ANTLR start "O"
    public final void mO() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:99:2: ( 'O' | 'o' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='O'||input.LA(1)=='o' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "O"

    // $ANTLR start "P"
    public final void mP() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:102:2: ( 'P' | 'p' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='P'||input.LA(1)=='p' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "P"

    // $ANTLR start "Q"
    public final void mQ() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:105:2: ( 'Q' | 'q' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='Q'||input.LA(1)=='q' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "Q"

    // $ANTLR start "R"
    public final void mR() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:108:2: ( 'R' | 'r' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='R'||input.LA(1)=='r' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "R"

    // $ANTLR start "S"
    public final void mS() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:111:2: ( 'S' | 's' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='S'||input.LA(1)=='s' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "S"

    // $ANTLR start "T"
    public final void mT() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:114:2: ( 'T' | 't' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='T'||input.LA(1)=='t' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "T"

    // $ANTLR start "U"
    public final void mU() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:117:2: ( 'U' | 'u' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='U'||input.LA(1)=='u' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "U"

    // $ANTLR start "V"
    public final void mV() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:120:2: ( 'V' | 'v' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='V'||input.LA(1)=='v' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "V"

    // $ANTLR start "W"
    public final void mW() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:123:2: ( 'W' | 'w' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='W'||input.LA(1)=='w' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "W"

    // $ANTLR start "X"
    public final void mX() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:126:2: ( 'X' | 'x' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='X'||input.LA(1)=='x' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "X"

    // $ANTLR start "Y"
    public final void mY() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:129:2: ( 'Y' | 'y' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='Y'||input.LA(1)=='y' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "Y"

    // $ANTLR start "Z"
    public final void mZ() throws RecognitionException {
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:132:2: ( 'Z' | 'z' )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:
            {
            if ( input.LA(1)=='Z'||input.LA(1)=='z' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "Z"

    // $ANTLR start "WS"
    public final void mWS() throws RecognitionException {
        try {
            int _type = WS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:133:6: ( ( ' ' | '\\t' | '\\r' | '\\n' ) )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:133:11: ( ' ' | '\\t' | '\\r' | '\\n' )
            {
            if ( (input.LA(1) >= '\t' && input.LA(1) <= '\n')||input.LA(1)=='\r'||input.LA(1)==' ' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            _channel=HIDDEN;

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "WS"

    public void mTokens() throws RecognitionException {
        // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:8: ( KW_AND | KW_LIKE | KW_NOT | KW_OR | KW_NULL | KW_TRUE | KW_FALSE | KW_BETWEEN | LPAREN | RPAREN | EQ | LEQ | LTH | GEQ | GTH | NOT_EQ | MOD | DOT | ID | QUOTED_STRING | DOUBLEQUOTED_STRING | INT | FLOAT | WS )
        int alt16=24;
        alt16 = dfa16.predict(input);
        switch (alt16) {
            case 1 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:10: KW_AND
                {
                mKW_AND(); 


                }
                break;
            case 2 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:17: KW_LIKE
                {
                mKW_LIKE(); 


                }
                break;
            case 3 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:25: KW_NOT
                {
                mKW_NOT(); 


                }
                break;
            case 4 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:32: KW_OR
                {
                mKW_OR(); 


                }
                break;
            case 5 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:38: KW_NULL
                {
                mKW_NULL(); 


                }
                break;
            case 6 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:46: KW_TRUE
                {
                mKW_TRUE(); 


                }
                break;
            case 7 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:54: KW_FALSE
                {
                mKW_FALSE(); 


                }
                break;
            case 8 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:63: KW_BETWEEN
                {
                mKW_BETWEEN(); 


                }
                break;
            case 9 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:74: LPAREN
                {
                mLPAREN(); 


                }
                break;
            case 10 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:81: RPAREN
                {
                mRPAREN(); 


                }
                break;
            case 11 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:88: EQ
                {
                mEQ(); 


                }
                break;
            case 12 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:91: LEQ
                {
                mLEQ(); 


                }
                break;
            case 13 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:95: LTH
                {
                mLTH(); 


                }
                break;
            case 14 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:99: GEQ
                {
                mGEQ(); 


                }
                break;
            case 15 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:103: GTH
                {
                mGTH(); 


                }
                break;
            case 16 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:107: NOT_EQ
                {
                mNOT_EQ(); 


                }
                break;
            case 17 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:114: MOD
                {
                mMOD(); 


                }
                break;
            case 18 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:118: DOT
                {
                mDOT(); 


                }
                break;
            case 19 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:122: ID
                {
                mID(); 


                }
                break;
            case 20 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:125: QUOTED_STRING
                {
                mQUOTED_STRING(); 


                }
                break;
            case 21 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:139: DOUBLEQUOTED_STRING
                {
                mDOUBLEQUOTED_STRING(); 


                }
                break;
            case 22 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:159: INT
                {
                mINT(); 


                }
                break;
            case 23 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:163: FLOAT
                {
                mFLOAT(); 


                }
                break;
            case 24 :
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterLexer.g:1:169: WS
                {
                mWS(); 


                }
                break;

        }

    }


    protected DFA13 dfa13 = new DFA13(this);
    protected DFA16 dfa16 = new DFA16(this);
    static final String DFA13_eotS =
        "\5\uffff";
    static final String DFA13_eofS =
        "\5\uffff";
    static final String DFA13_minS =
        "\2\56\3\uffff";
    static final String DFA13_maxS =
        "\1\71\1\145\3\uffff";
    static final String DFA13_acceptS =
        "\2\uffff\1\2\1\1\1\3";
    static final String DFA13_specialS =
        "\5\uffff}>";
    static final String[] DFA13_transitionS = {
            "\1\2\1\uffff\12\1",
            "\1\3\1\uffff\12\1\13\uffff\1\4\37\uffff\1\4",
            "",
            "",
            ""
    };

    static final short[] DFA13_eot = DFA.unpackEncodedString(DFA13_eotS);
    static final short[] DFA13_eof = DFA.unpackEncodedString(DFA13_eofS);
    static final char[] DFA13_min = DFA.unpackEncodedStringToUnsignedChars(DFA13_minS);
    static final char[] DFA13_max = DFA.unpackEncodedStringToUnsignedChars(DFA13_maxS);
    static final short[] DFA13_accept = DFA.unpackEncodedString(DFA13_acceptS);
    static final short[] DFA13_special = DFA.unpackEncodedString(DFA13_specialS);
    static final short[][] DFA13_transition;

    static {
        int numStates = DFA13_transitionS.length;
        DFA13_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA13_transition[i] = DFA.unpackEncodedString(DFA13_transitionS[i]);
        }
    }

    class DFA13 extends DFA {

        public DFA13(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 13;
            this.eot = DFA13_eot;
            this.eof = DFA13_eof;
            this.min = DFA13_min;
            this.max = DFA13_max;
            this.accept = DFA13_accept;
            this.special = DFA13_special;
            this.transition = DFA13_transition;
        }
        public String getDescription() {
            return "42:1: FLOAT : ( ( '0' .. '9' )+ '.' ( '0' .. '9' )* ( EXPONENT )? | '.' ( '0' .. '9' )+ ( EXPONENT )? | ( '0' .. '9' )+ EXPONENT );";
        }
    }
    static final String DFA16_eotS =
        "\1\uffff\7\20\3\uffff\1\37\1\41\2\uffff\1\42\1\uffff\1\20\2\uffff"+
        "\1\44\1\uffff\4\20\1\51\3\20\7\uffff\1\55\1\20\1\57\1\20\1\uffff"+
        "\3\20\1\uffff\1\64\1\uffff\1\65\1\66\2\20\3\uffff\1\71\1\20\1\uffff"+
        "\1\20\1\74\1\uffff";
    static final String DFA16_eofS =
        "\75\uffff";
    static final String DFA16_minS =
        "\1\11\1\116\1\111\1\47\2\122\1\101\1\105\3\uffff\2\75\2\uffff\1"+
        "\60\1\uffff\1\117\2\uffff\1\56\1\uffff\1\104\1\113\1\124\1\114\1"+
        "\60\1\125\1\114\1\124\7\uffff\1\60\1\105\1\60\1\114\1\uffff\1\105"+
        "\1\123\1\127\1\uffff\1\60\1\uffff\2\60\2\105\3\uffff\1\60\1\105"+
        "\1\uffff\1\116\1\60\1\uffff";
    static final String DFA16_maxS =
        "\1\172\1\156\1\151\1\165\2\162\1\141\1\145\3\uffff\1\76\1\75\2\uffff"+
        "\1\71\1\uffff\1\165\2\uffff\1\145\1\uffff\1\144\1\153\1\164\1\154"+
        "\1\172\1\165\1\154\1\164\7\uffff\1\172\1\145\1\172\1\154\1\uffff"+
        "\1\145\1\163\1\167\1\uffff\1\172\1\uffff\2\172\2\145\3\uffff\1\172"+
        "\1\145\1\uffff\1\156\1\172\1\uffff";
    static final String DFA16_acceptS =
        "\10\uffff\1\11\1\12\1\13\2\uffff\1\20\1\21\1\uffff\1\23\1\uffff"+
        "\1\24\1\25\1\uffff\1\30\10\uffff\1\14\1\15\1\16\1\17\1\22\1\27\1"+
        "\26\4\uffff\1\4\3\uffff\1\1\1\uffff\1\3\4\uffff\1\2\1\5\1\6\2\uffff"+
        "\1\7\2\uffff\1\10";
    static final String DFA16_specialS =
        "\75\uffff}>";
    static final String[] DFA16_transitionS = {
            "\2\25\2\uffff\1\25\22\uffff\1\25\1\15\1\23\2\uffff\1\16\1\uffff"+
            "\1\22\1\10\1\11\4\uffff\1\17\1\uffff\12\24\2\uffff\1\13\1\12"+
            "\1\14\2\uffff\1\1\1\7\3\20\1\6\5\20\1\2\1\20\1\21\1\4\4\20\1"+
            "\5\6\20\3\uffff\1\15\2\uffff\1\1\1\7\3\20\1\6\5\20\1\2\1\20"+
            "\1\3\1\4\4\20\1\5\6\20",
            "\1\26\37\uffff\1\26",
            "\1\27\37\uffff\1\27",
            "\1\22\47\uffff\1\30\5\uffff\1\31\31\uffff\1\30\5\uffff\1\31",
            "\1\32\37\uffff\1\32",
            "\1\33\37\uffff\1\33",
            "\1\34\37\uffff\1\34",
            "\1\35\37\uffff\1\35",
            "",
            "",
            "",
            "\1\36\1\15",
            "\1\40",
            "",
            "",
            "\12\43",
            "",
            "\1\30\5\uffff\1\31\31\uffff\1\30\5\uffff\1\31",
            "",
            "",
            "\1\43\1\uffff\12\24\13\uffff\1\43\37\uffff\1\43",
            "",
            "\1\45\37\uffff\1\45",
            "\1\46\37\uffff\1\46",
            "\1\47\37\uffff\1\47",
            "\1\50\37\uffff\1\50",
            "\12\20\7\uffff\32\20\4\uffff\1\20\1\uffff\32\20",
            "\1\52\37\uffff\1\52",
            "\1\53\37\uffff\1\53",
            "\1\54\37\uffff\1\54",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "\12\20\7\uffff\32\20\4\uffff\1\20\1\uffff\32\20",
            "\1\56\37\uffff\1\56",
            "\12\20\7\uffff\32\20\4\uffff\1\20\1\uffff\32\20",
            "\1\60\37\uffff\1\60",
            "",
            "\1\61\37\uffff\1\61",
            "\1\62\37\uffff\1\62",
            "\1\63\37\uffff\1\63",
            "",
            "\12\20\7\uffff\32\20\4\uffff\1\20\1\uffff\32\20",
            "",
            "\12\20\7\uffff\32\20\4\uffff\1\20\1\uffff\32\20",
            "\12\20\7\uffff\32\20\4\uffff\1\20\1\uffff\32\20",
            "\1\67\37\uffff\1\67",
            "\1\70\37\uffff\1\70",
            "",
            "",
            "",
            "\12\20\7\uffff\32\20\4\uffff\1\20\1\uffff\32\20",
            "\1\72\37\uffff\1\72",
            "",
            "\1\73\37\uffff\1\73",
            "\12\20\7\uffff\32\20\4\uffff\1\20\1\uffff\32\20",
            ""
    };

    static final short[] DFA16_eot = DFA.unpackEncodedString(DFA16_eotS);
    static final short[] DFA16_eof = DFA.unpackEncodedString(DFA16_eofS);
    static final char[] DFA16_min = DFA.unpackEncodedStringToUnsignedChars(DFA16_minS);
    static final char[] DFA16_max = DFA.unpackEncodedStringToUnsignedChars(DFA16_maxS);
    static final short[] DFA16_accept = DFA.unpackEncodedString(DFA16_acceptS);
    static final short[] DFA16_special = DFA.unpackEncodedString(DFA16_specialS);
    static final short[][] DFA16_transition;

    static {
        int numStates = DFA16_transitionS.length;
        DFA16_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA16_transition[i] = DFA.unpackEncodedString(DFA16_transitionS[i]);
        }
    }

    class DFA16 extends DFA {

        public DFA16(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 16;
            this.eot = DFA16_eot;
            this.eof = DFA16_eof;
            this.min = DFA16_min;
            this.max = DFA16_max;
            this.accept = DFA16_accept;
            this.special = DFA16_special;
            this.transition = DFA16_transition;
        }
        public String getDescription() {
            return "1:1: Tokens : ( KW_AND | KW_LIKE | KW_NOT | KW_OR | KW_NULL | KW_TRUE | KW_FALSE | KW_BETWEEN | LPAREN | RPAREN | EQ | LEQ | LTH | GEQ | GTH | NOT_EQ | MOD | DOT | ID | QUOTED_STRING | DOUBLEQUOTED_STRING | INT | FLOAT | WS );";
        }
    }
 

}