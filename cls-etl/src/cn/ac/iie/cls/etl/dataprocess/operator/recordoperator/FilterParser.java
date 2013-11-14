package cn.ac.iie.cls.etl.dataprocess.operator.recordoperator;
// $ANTLR 3.4 G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g 2013-08-22 08:49:20

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.antlr.runtime.tree.*;


/**
	@auther :	hanbing
	date	:	2013-08-09
**/
@SuppressWarnings({"all", "warnings", "unchecked"})
public class FilterParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "A", "B", "C", "D", "DOT", "DOUBLEQUOTED_STRING", "Digit", "E", "EQ", "ESC", "EXPONENT", "F", "FLOAT", "G", "GEQ", "GTH", "H", "I", "ID", "INT", "J", "K", "KW_AND", "KW_BETWEEN", "KW_FALSE", "KW_LIKE", "KW_NOT", "KW_NULL", "KW_OR", "KW_TRUE", "L", "LEQ", "LPAREN", "LTH", "Letter", "M", "MOD", "N", "NOT_EQ", "O", "P", "Q", "QUOTED_STRING", "R", "RPAREN", "S", "T", "U", "V", "W", "WS", "X", "Y", "Z", "EXP_BOOL", "TOK_DOUBLEQUOTED_STRING", "TOK_FLOAT", "TOK_ID", "TOK_INT", "TOK_KW_AND", "TOK_KW_BETWEEN", "TOK_KW_FALSE", "TOK_KW_LIKE", "TOK_KW_NOT", "TOK_KW_NULL", "TOK_KW_OR", "TOK_KW_TRUE", "TOK_OP_DOT", "TOK_OP_EQ", "TOK_OP_GEQ", "TOK_OP_GTH", "TOK_OP_LEQ", "TOK_OP_LTH", "TOK_OP_MOD", "TOK_OP_NOT_EQ", "TOK_QUOTED_STRING"
    };

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
    public static final int EXP_BOOL=58;
    public static final int TOK_DOUBLEQUOTED_STRING=59;
    public static final int TOK_FLOAT=60;
    public static final int TOK_ID=61;
    public static final int TOK_INT=62;
    public static final int TOK_KW_AND=63;
    public static final int TOK_KW_BETWEEN=64;
    public static final int TOK_KW_FALSE=65;
    public static final int TOK_KW_LIKE=66;
    public static final int TOK_KW_NOT=67;
    public static final int TOK_KW_NULL=68;
    public static final int TOK_KW_OR=69;
    public static final int TOK_KW_TRUE=70;
    public static final int TOK_OP_DOT=71;
    public static final int TOK_OP_EQ=72;
    public static final int TOK_OP_GEQ=73;
    public static final int TOK_OP_GTH=74;
    public static final int TOK_OP_LEQ=75;
    public static final int TOK_OP_LTH=76;
    public static final int TOK_OP_MOD=77;
    public static final int TOK_OP_NOT_EQ=78;
    public static final int TOK_QUOTED_STRING=79;

    // delegates
    public Parser[] getDelegates() {
        return new Parser[] {};
    }

    // delegators


    public FilterParser(TokenStream input) {
        this(input, new RecognizerSharedState());
    }
    public FilterParser(TokenStream input, RecognizerSharedState state) {
        super(input, state);
    }

protected TreeAdaptor adaptor = new CommonTreeAdaptor();

public void setTreeAdaptor(TreeAdaptor adaptor) {
    this.adaptor = adaptor;
}
public TreeAdaptor getTreeAdaptor() {
    return adaptor;
}
    public String[] getTokenNames() { return FilterParser.tokenNames; }
    public String getGrammarFileName() { return "G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g"; }


    public static class program_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "program"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:46:1: program : ( stat )+ ;
    public final FilterParser.program_return program() throws RecognitionException {
        FilterParser.program_return retval = new FilterParser.program_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        FilterParser.stat_return stat1 =null;



        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:46:11: ( ( stat )+ )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:46:14: ( stat )+
            {
            root_0 = (CommonTree)adaptor.nil();


            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:46:14: ( stat )+
            int cnt1=0;
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( (LA1_0==DOUBLEQUOTED_STRING||LA1_0==ID||LA1_0==KW_NOT||LA1_0==LPAREN) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:46:14: stat
            	    {
            	    pushFollow(FOLLOW_stat_in_program180);
            	    stat1=stat();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, stat1.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt1 >= 1 ) break loop1;
            	    if (state.backtracking>0) {state.failed=true; return retval;}
                        EarlyExitException eee =
                            new EarlyExitException(1, input);
                        throw eee;
                }
                cnt1++;
            } while (true);


            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "program"


    public static class stat_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "stat"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:48:1: stat : filter_condition ;
    public final FilterParser.stat_return stat() throws RecognitionException {
        FilterParser.stat_return retval = new FilterParser.stat_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        FilterParser.filter_condition_return filter_condition2 =null;



        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:48:8: ( filter_condition )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:48:11: filter_condition
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_filter_condition_in_stat193);
            filter_condition2=filter_condition();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, filter_condition2.getTree());

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "stat"


    public static class filter_expressions_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "filter_expressions"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:50:1: filter_expressions : filter_expression ;
    public final FilterParser.filter_expressions_return filter_expressions() throws RecognitionException {
        FilterParser.filter_expressions_return retval = new FilterParser.filter_expressions_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        FilterParser.filter_expression_return filter_expression3 =null;



        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:51:2: ( filter_expression )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:51:4: filter_expression
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_filter_expression_in_filter_expressions203);
            filter_expression3=filter_expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, filter_expression3.getTree());

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "filter_expressions"


    public static class filter_expression_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "filter_expression"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:54:1: filter_expression : expr_expr ;
    public final FilterParser.filter_expression_return filter_expression() throws RecognitionException {
        FilterParser.filter_expression_return retval = new FilterParser.filter_expression_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        FilterParser.expr_expr_return expr_expr4 =null;



        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:55:2: ( expr_expr )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:55:4: expr_expr
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_expr_expr_in_filter_expression215);
            expr_expr4=expr_expr();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, expr_expr4.getTree());

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "filter_expression"


    public static class expr_expr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "expr_expr"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:58:1: expr_expr : ( KW_NULL | QUOTED_STRING | INT | FLOAT | KW_TRUE | KW_FALSE );
    public final FilterParser.expr_expr_return expr_expr() throws RecognitionException {
        FilterParser.expr_expr_return retval = new FilterParser.expr_expr_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token set5=null;

        CommonTree set5_tree=null;

        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:59:2: ( KW_NULL | QUOTED_STRING | INT | FLOAT | KW_TRUE | KW_FALSE )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:
            {
            root_0 = (CommonTree)adaptor.nil();


            set5=(Token)input.LT(1);

            if ( input.LA(1)==FLOAT||input.LA(1)==INT||input.LA(1)==KW_FALSE||input.LA(1)==KW_NULL||input.LA(1)==KW_TRUE||input.LA(1)==QUOTED_STRING ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, 
                (CommonTree)adaptor.create(set5)
                );
                state.errorRecovery=false;
                state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "expr_expr"


    public static class digit_expr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "digit_expr"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:62:1: digit_expr : ( INT | FLOAT );
    public final FilterParser.digit_expr_return digit_expr() throws RecognitionException {
        FilterParser.digit_expr_return retval = new FilterParser.digit_expr_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token set6=null;

        CommonTree set6_tree=null;

        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:63:2: ( INT | FLOAT )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:
            {
            root_0 = (CommonTree)adaptor.nil();


            set6=(Token)input.LT(1);

            if ( input.LA(1)==FLOAT||input.LA(1)==INT ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, 
                (CommonTree)adaptor.create(set6)
                );
                state.errorRecovery=false;
                state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "digit_expr"


    public static class bool_expr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "bool_expr"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:66:1: bool_expr : ( KW_TRUE | KW_FALSE );
    public final FilterParser.bool_expr_return bool_expr() throws RecognitionException {
        FilterParser.bool_expr_return retval = new FilterParser.bool_expr_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token set7=null;

        CommonTree set7_tree=null;

        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:67:2: ( KW_TRUE | KW_FALSE )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:
            {
            root_0 = (CommonTree)adaptor.nil();


            set7=(Token)input.LT(1);

            if ( input.LA(1)==KW_FALSE||input.LA(1)==KW_TRUE ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, 
                (CommonTree)adaptor.create(set7)
                );
                state.errorRecovery=false;
                state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "bool_expr"


    public static class string_expr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "string_expr"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:70:1: string_expr : ( QUOTED_STRING | KW_NULL );
    public final FilterParser.string_expr_return string_expr() throws RecognitionException {
        FilterParser.string_expr_return retval = new FilterParser.string_expr_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token set8=null;

        CommonTree set8_tree=null;

        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:71:2: ( QUOTED_STRING | KW_NULL )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:
            {
            root_0 = (CommonTree)adaptor.nil();


            set8=(Token)input.LT(1);

            if ( input.LA(1)==KW_NULL||input.LA(1)==QUOTED_STRING ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, 
                (CommonTree)adaptor.create(set8)
                );
                state.errorRecovery=false;
                state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "string_expr"


    public static class filter_condition_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "filter_condition"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:76:1: filter_condition : condition_or ;
    public final FilterParser.filter_condition_return filter_condition() throws RecognitionException {
        FilterParser.filter_condition_return retval = new FilterParser.filter_condition_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        FilterParser.condition_or_return condition_or9 =null;



        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:77:2: ( condition_or )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:77:4: condition_or
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_condition_or_in_filter_condition307);
            condition_or9=condition_or();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, condition_or9.getTree());

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "filter_condition"


    public static class condition_or_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "condition_or"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:81:1: condition_or : condition_and ( KW_OR condition_and )* -> ^( TOK_KW_OR condition_and ( condition_and )* ) ;
    public final FilterParser.condition_or_return condition_or() throws RecognitionException {
        FilterParser.condition_or_return retval = new FilterParser.condition_or_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_OR11=null;
        FilterParser.condition_and_return condition_and10 =null;

        FilterParser.condition_and_return condition_and12 =null;


        CommonTree KW_OR11_tree=null;
        RewriteRuleTokenStream stream_KW_OR=new RewriteRuleTokenStream(adaptor,"token KW_OR");
        RewriteRuleSubtreeStream stream_condition_and=new RewriteRuleSubtreeStream(adaptor,"rule condition_and");
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:82:2: ( condition_and ( KW_OR condition_and )* -> ^( TOK_KW_OR condition_and ( condition_and )* ) )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:82:4: condition_and ( KW_OR condition_and )*
            {
            pushFollow(FOLLOW_condition_and_in_condition_or319);
            condition_and10=condition_and();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_condition_and.add(condition_and10.getTree());

            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:82:18: ( KW_OR condition_and )*
            loop2:
            do {
                int alt2=2;
                int LA2_0 = input.LA(1);

                if ( (LA2_0==KW_OR) ) {
                    alt2=1;
                }


                switch (alt2) {
            	case 1 :
            	    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:82:20: KW_OR condition_and
            	    {
            	    KW_OR11=(Token)match(input,KW_OR,FOLLOW_KW_OR_in_condition_or323); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_KW_OR.add(KW_OR11);


            	    pushFollow(FOLLOW_condition_and_in_condition_or325);
            	    condition_and12=condition_and();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_condition_and.add(condition_and12.getTree());

            	    }
            	    break;

            	default :
            	    break loop2;
                }
            } while (true);


            // AST REWRITE
            // elements: condition_and, condition_and
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 82:43: -> ^( TOK_KW_OR condition_and ( condition_and )* )
            {
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:82:46: ^( TOK_KW_OR condition_and ( condition_and )* )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_KW_OR, "TOK_KW_OR")
                , root_1);

                adaptor.addChild(root_1, stream_condition_and.nextTree());

                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:82:72: ( condition_and )*
                while ( stream_condition_and.hasNext() ) {
                    adaptor.addChild(root_1, stream_condition_and.nextTree());

                }
                stream_condition_and.reset();

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "condition_or"


    public static class condition_and_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "condition_and"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:85:1: condition_and : condition_factor ( KW_AND condition_factor )* -> ^( TOK_KW_AND condition_factor ( condition_factor )* ) ;
    public final FilterParser.condition_and_return condition_and() throws RecognitionException {
        FilterParser.condition_and_return retval = new FilterParser.condition_and_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_AND14=null;
        FilterParser.condition_factor_return condition_factor13 =null;

        FilterParser.condition_factor_return condition_factor15 =null;


        CommonTree KW_AND14_tree=null;
        RewriteRuleTokenStream stream_KW_AND=new RewriteRuleTokenStream(adaptor,"token KW_AND");
        RewriteRuleSubtreeStream stream_condition_factor=new RewriteRuleSubtreeStream(adaptor,"rule condition_factor");
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:86:2: ( condition_factor ( KW_AND condition_factor )* -> ^( TOK_KW_AND condition_factor ( condition_factor )* ) )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:86:4: condition_factor ( KW_AND condition_factor )*
            {
            pushFollow(FOLLOW_condition_factor_in_condition_and350);
            condition_factor13=condition_factor();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_condition_factor.add(condition_factor13.getTree());

            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:86:21: ( KW_AND condition_factor )*
            loop3:
            do {
                int alt3=2;
                int LA3_0 = input.LA(1);

                if ( (LA3_0==KW_AND) ) {
                    alt3=1;
                }


                switch (alt3) {
            	case 1 :
            	    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:86:23: KW_AND condition_factor
            	    {
            	    KW_AND14=(Token)match(input,KW_AND,FOLLOW_KW_AND_in_condition_and354); if (state.failed) return retval; 
            	    if ( state.backtracking==0 ) stream_KW_AND.add(KW_AND14);


            	    pushFollow(FOLLOW_condition_factor_in_condition_and356);
            	    condition_factor15=condition_factor();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) stream_condition_factor.add(condition_factor15.getTree());

            	    }
            	    break;

            	default :
            	    break loop3;
                }
            } while (true);


            // AST REWRITE
            // elements: condition_factor, condition_factor
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 86:50: -> ^( TOK_KW_AND condition_factor ( condition_factor )* )
            {
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:86:53: ^( TOK_KW_AND condition_factor ( condition_factor )* )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_KW_AND, "TOK_KW_AND")
                , root_1);

                adaptor.addChild(root_1, stream_condition_factor.nextTree());

                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:86:83: ( condition_factor )*
                while ( stream_condition_factor.hasNext() ) {
                    adaptor.addChild(root_1, stream_condition_factor.nextTree());

                }
                stream_condition_factor.reset();

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "condition_and"


    public static class condition_factor_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "condition_factor"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:89:1: condition_factor : ( KW_NOT condition_expr | condition_expr );
    public final FilterParser.condition_factor_return condition_factor() throws RecognitionException {
        FilterParser.condition_factor_return retval = new FilterParser.condition_factor_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_NOT16=null;
        FilterParser.condition_expr_return condition_expr17 =null;

        FilterParser.condition_expr_return condition_expr18 =null;


        CommonTree KW_NOT16_tree=null;

        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:90:2: ( KW_NOT condition_expr | condition_expr )
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( (LA4_0==KW_NOT) ) {
                alt4=1;
            }
            else if ( (LA4_0==DOUBLEQUOTED_STRING||LA4_0==ID||LA4_0==LPAREN) ) {
                alt4=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 4, 0, input);

                throw nvae;

            }
            switch (alt4) {
                case 1 :
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:90:4: KW_NOT condition_expr
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    KW_NOT16=(Token)match(input,KW_NOT,FOLLOW_KW_NOT_in_condition_factor382); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    KW_NOT16_tree = 
                    (CommonTree)adaptor.create(KW_NOT16)
                    ;
                    adaptor.addChild(root_0, KW_NOT16_tree);
                    }

                    pushFollow(FOLLOW_condition_expr_in_condition_factor384);
                    condition_expr17=condition_expr();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, condition_expr17.getTree());

                    }
                    break;
                case 2 :
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:91:4: condition_expr
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_condition_expr_in_condition_factor389);
                    condition_expr18=condition_expr();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, condition_expr18.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "condition_factor"


    public static class condition_expr_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "condition_expr"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:93:1: condition_expr : ( predicate | boolean_predicand );
    public final FilterParser.condition_expr_return condition_expr() throws RecognitionException {
        FilterParser.condition_expr_return retval = new FilterParser.condition_expr_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        FilterParser.predicate_return predicate19 =null;

        FilterParser.boolean_predicand_return boolean_predicand20 =null;



        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:94:2: ( predicate | boolean_predicand )
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( (LA5_0==DOUBLEQUOTED_STRING||LA5_0==ID) ) {
                alt5=1;
            }
            else if ( (LA5_0==LPAREN) ) {
                alt5=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                NoViableAltException nvae =
                    new NoViableAltException("", 5, 0, input);

                throw nvae;

            }
            switch (alt5) {
                case 1 :
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:94:4: predicate
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_predicate_in_condition_expr410);
                    predicate19=predicate();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, predicate19.getTree());

                    }
                    break;
                case 2 :
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:95:4: boolean_predicand
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_boolean_predicand_in_condition_expr415);
                    boolean_predicand20=boolean_predicand();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, boolean_predicand20.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "condition_expr"


    public static class boolean_predicand_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "boolean_predicand"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:98:1: boolean_predicand : parenthesized_boolean_value_expression ;
    public final FilterParser.boolean_predicand_return boolean_predicand() throws RecognitionException {
        FilterParser.boolean_predicand_return retval = new FilterParser.boolean_predicand_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        FilterParser.parenthesized_boolean_value_expression_return parenthesized_boolean_value_expression21 =null;



        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:99:2: ( parenthesized_boolean_value_expression )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:99:4: parenthesized_boolean_value_expression
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_parenthesized_boolean_value_expression_in_boolean_predicand427);
            parenthesized_boolean_value_expression21=parenthesized_boolean_value_expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, parenthesized_boolean_value_expression21.getTree());

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "boolean_predicand"


    public static class parenthesized_boolean_value_expression_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "parenthesized_boolean_value_expression"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:103:1: parenthesized_boolean_value_expression : LPAREN condition_or RPAREN -> condition_or ;
    public final FilterParser.parenthesized_boolean_value_expression_return parenthesized_boolean_value_expression() throws RecognitionException {
        FilterParser.parenthesized_boolean_value_expression_return retval = new FilterParser.parenthesized_boolean_value_expression_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token LPAREN22=null;
        Token RPAREN24=null;
        FilterParser.condition_or_return condition_or23 =null;


        CommonTree LPAREN22_tree=null;
        CommonTree RPAREN24_tree=null;
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleSubtreeStream stream_condition_or=new RewriteRuleSubtreeStream(adaptor,"rule condition_or");
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:105:2: ( LPAREN condition_or RPAREN -> condition_or )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:105:5: LPAREN condition_or RPAREN
            {
            LPAREN22=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_parenthesized_boolean_value_expression444); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_LPAREN.add(LPAREN22);


            pushFollow(FOLLOW_condition_or_in_parenthesized_boolean_value_expression446);
            condition_or23=condition_or();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_condition_or.add(condition_or23.getTree());

            RPAREN24=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_parenthesized_boolean_value_expression449); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_RPAREN.add(RPAREN24);


            // AST REWRITE
            // elements: condition_or
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 105:33: -> condition_or
            {
                adaptor.addChild(root_0, stream_condition_or.nextTree());

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "parenthesized_boolean_value_expression"


    public static class predicate_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "predicate"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:108:1: predicate : ( condition_comparison1 | condition_comparison2 | condition_like | condition_between );
    public final FilterParser.predicate_return predicate() throws RecognitionException {
        FilterParser.predicate_return retval = new FilterParser.predicate_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        FilterParser.condition_comparison1_return condition_comparison125 =null;

        FilterParser.condition_comparison2_return condition_comparison226 =null;

        FilterParser.condition_like_return condition_like27 =null;

        FilterParser.condition_between_return condition_between28 =null;



        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:110:2: ( condition_comparison1 | condition_comparison2 | condition_like | condition_between )
            int alt6=4;
            alt6 = dfa6.predict(input);
            switch (alt6) {
                case 1 :
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:110:4: condition_comparison1
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_condition_comparison1_in_predicate466);
                    condition_comparison125=condition_comparison1();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, condition_comparison125.getTree());

                    }
                    break;
                case 2 :
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:111:4: condition_comparison2
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_condition_comparison2_in_predicate471);
                    condition_comparison226=condition_comparison2();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, condition_comparison226.getTree());

                    }
                    break;
                case 3 :
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:112:4: condition_like
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_condition_like_in_predicate476);
                    condition_like27=condition_like();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, condition_like27.getTree());

                    }
                    break;
                case 4 :
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:113:4: condition_between
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_condition_between_in_predicate481);
                    condition_between28=condition_between();

                    state._fsp--;
                    if (state.failed) return retval;
                    if ( state.backtracking==0 ) adaptor.addChild(root_0, condition_between28.getTree());

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "predicate"


    public static class condition_between_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "condition_between"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:116:1: condition_between : column_spec between_predicate_part_2 ;
    public final FilterParser.condition_between_return condition_between() throws RecognitionException {
        FilterParser.condition_between_return retval = new FilterParser.condition_between_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        FilterParser.column_spec_return column_spec29 =null;

        FilterParser.between_predicate_part_2_return between_predicate_part_230 =null;



        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:117:2: ( column_spec between_predicate_part_2 )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:117:4: column_spec between_predicate_part_2
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_column_spec_in_condition_between493);
            column_spec29=column_spec();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, column_spec29.getTree());

            pushFollow(FOLLOW_between_predicate_part_2_in_condition_between495);
            between_predicate_part_230=between_predicate_part_2();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, between_predicate_part_230.getTree());

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "condition_between"


    public static class between_predicate_part_2_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "between_predicate_part_2"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:120:1: between_predicate_part_2 : ( KW_NOT )? KW_BETWEEN filter_expression KW_AND filter_expression ;
    public final FilterParser.between_predicate_part_2_return between_predicate_part_2() throws RecognitionException {
        FilterParser.between_predicate_part_2_return retval = new FilterParser.between_predicate_part_2_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_NOT31=null;
        Token KW_BETWEEN32=null;
        Token KW_AND34=null;
        FilterParser.filter_expression_return filter_expression33 =null;

        FilterParser.filter_expression_return filter_expression35 =null;


        CommonTree KW_NOT31_tree=null;
        CommonTree KW_BETWEEN32_tree=null;
        CommonTree KW_AND34_tree=null;

        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:122:2: ( ( KW_NOT )? KW_BETWEEN filter_expression KW_AND filter_expression )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:122:5: ( KW_NOT )? KW_BETWEEN filter_expression KW_AND filter_expression
            {
            root_0 = (CommonTree)adaptor.nil();


            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:122:5: ( KW_NOT )?
            int alt7=2;
            int LA7_0 = input.LA(1);

            if ( (LA7_0==KW_NOT) ) {
                alt7=1;
            }
            switch (alt7) {
                case 1 :
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:122:7: KW_NOT
                    {
                    KW_NOT31=(Token)match(input,KW_NOT,FOLLOW_KW_NOT_in_between_predicate_part_2512); if (state.failed) return retval;
                    if ( state.backtracking==0 ) {
                    KW_NOT31_tree = 
                    (CommonTree)adaptor.create(KW_NOT31)
                    ;
                    adaptor.addChild(root_0, KW_NOT31_tree);
                    }

                    }
                    break;

            }


            KW_BETWEEN32=(Token)match(input,KW_BETWEEN,FOLLOW_KW_BETWEEN_in_between_predicate_part_2517); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            KW_BETWEEN32_tree = 
            (CommonTree)adaptor.create(KW_BETWEEN32)
            ;
            adaptor.addChild(root_0, KW_BETWEEN32_tree);
            }

            pushFollow(FOLLOW_filter_expression_in_between_predicate_part_2519);
            filter_expression33=filter_expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, filter_expression33.getTree());

            KW_AND34=(Token)match(input,KW_AND,FOLLOW_KW_AND_in_between_predicate_part_2522); if (state.failed) return retval;
            if ( state.backtracking==0 ) {
            KW_AND34_tree = 
            (CommonTree)adaptor.create(KW_AND34)
            ;
            adaptor.addChild(root_0, KW_AND34_tree);
            }

            pushFollow(FOLLOW_filter_expression_in_between_predicate_part_2524);
            filter_expression35=filter_expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, filter_expression35.getTree());

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "between_predicate_part_2"


    public static class condition_like_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "condition_like"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:124:1: condition_like : column_spec ( KW_NOT )? KW_LIKE QUOTED_STRING -> ^( EXP_BOOL column_spec TOK_KW_LIKE QUOTED_STRING ) ;
    public final FilterParser.condition_like_return condition_like() throws RecognitionException {
        FilterParser.condition_like_return retval = new FilterParser.condition_like_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_NOT37=null;
        Token KW_LIKE38=null;
        Token QUOTED_STRING39=null;
        FilterParser.column_spec_return column_spec36 =null;


        CommonTree KW_NOT37_tree=null;
        CommonTree KW_LIKE38_tree=null;
        CommonTree QUOTED_STRING39_tree=null;
        RewriteRuleTokenStream stream_KW_LIKE=new RewriteRuleTokenStream(adaptor,"token KW_LIKE");
        RewriteRuleTokenStream stream_QUOTED_STRING=new RewriteRuleTokenStream(adaptor,"token QUOTED_STRING");
        RewriteRuleTokenStream stream_KW_NOT=new RewriteRuleTokenStream(adaptor,"token KW_NOT");
        RewriteRuleSubtreeStream stream_column_spec=new RewriteRuleSubtreeStream(adaptor,"rule column_spec");
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:125:2: ( column_spec ( KW_NOT )? KW_LIKE QUOTED_STRING -> ^( EXP_BOOL column_spec TOK_KW_LIKE QUOTED_STRING ) )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:125:4: column_spec ( KW_NOT )? KW_LIKE QUOTED_STRING
            {
            pushFollow(FOLLOW_column_spec_in_condition_like546);
            column_spec36=column_spec();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_column_spec.add(column_spec36.getTree());

            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:125:16: ( KW_NOT )?
            int alt8=2;
            int LA8_0 = input.LA(1);

            if ( (LA8_0==KW_NOT) ) {
                alt8=1;
            }
            switch (alt8) {
                case 1 :
                    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:125:18: KW_NOT
                    {
                    KW_NOT37=(Token)match(input,KW_NOT,FOLLOW_KW_NOT_in_condition_like550); if (state.failed) return retval; 
                    if ( state.backtracking==0 ) stream_KW_NOT.add(KW_NOT37);


                    }
                    break;

            }


            KW_LIKE38=(Token)match(input,KW_LIKE,FOLLOW_KW_LIKE_in_condition_like555); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_KW_LIKE.add(KW_LIKE38);


            QUOTED_STRING39=(Token)match(input,QUOTED_STRING,FOLLOW_QUOTED_STRING_in_condition_like557); if (state.failed) return retval; 
            if ( state.backtracking==0 ) stream_QUOTED_STRING.add(QUOTED_STRING39);


            // AST REWRITE
            // elements: QUOTED_STRING, column_spec
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 125:50: -> ^( EXP_BOOL column_spec TOK_KW_LIKE QUOTED_STRING )
            {
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:125:53: ^( EXP_BOOL column_spec TOK_KW_LIKE QUOTED_STRING )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(EXP_BOOL, "EXP_BOOL")
                , root_1);

                adaptor.addChild(root_1, stream_column_spec.nextTree());

                adaptor.addChild(root_1, 
                (CommonTree)adaptor.create(TOK_KW_LIKE, "TOK_KW_LIKE")
                );

                adaptor.addChild(root_1, 
                stream_QUOTED_STRING.nextNode()
                );

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "condition_like"


    public static class condition_comparison1_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "condition_comparison1"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:128:1: condition_comparison1 : column_spec equal_char filter_expression -> ^( EXP_BOOL column_spec equal_char filter_expression ) ;
    public final FilterParser.condition_comparison1_return condition_comparison1() throws RecognitionException {
        FilterParser.condition_comparison1_return retval = new FilterParser.condition_comparison1_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        FilterParser.column_spec_return column_spec40 =null;

        FilterParser.equal_char_return equal_char41 =null;

        FilterParser.filter_expression_return filter_expression42 =null;


        RewriteRuleSubtreeStream stream_column_spec=new RewriteRuleSubtreeStream(adaptor,"rule column_spec");
        RewriteRuleSubtreeStream stream_filter_expression=new RewriteRuleSubtreeStream(adaptor,"rule filter_expression");
        RewriteRuleSubtreeStream stream_equal_char=new RewriteRuleSubtreeStream(adaptor,"rule equal_char");
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:129:2: ( column_spec equal_char filter_expression -> ^( EXP_BOOL column_spec equal_char filter_expression ) )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:129:4: column_spec equal_char filter_expression
            {
            pushFollow(FOLLOW_column_spec_in_condition_comparison1582);
            column_spec40=column_spec();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_column_spec.add(column_spec40.getTree());

            pushFollow(FOLLOW_equal_char_in_condition_comparison1584);
            equal_char41=equal_char();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_equal_char.add(equal_char41.getTree());

            pushFollow(FOLLOW_filter_expression_in_condition_comparison1586);
            filter_expression42=filter_expression();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_filter_expression.add(filter_expression42.getTree());

            // AST REWRITE
            // elements: column_spec, filter_expression, equal_char
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 129:45: -> ^( EXP_BOOL column_spec equal_char filter_expression )
            {
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:129:48: ^( EXP_BOOL column_spec equal_char filter_expression )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(EXP_BOOL, "EXP_BOOL")
                , root_1);

                adaptor.addChild(root_1, stream_column_spec.nextTree());

                adaptor.addChild(root_1, stream_equal_char.nextTree());

                adaptor.addChild(root_1, stream_filter_expression.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "condition_comparison1"


    public static class condition_comparison2_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "condition_comparison2"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:132:1: condition_comparison2 : column_spec greatless_char digit_expr -> ^( EXP_BOOL column_spec greatless_char digit_expr ) ;
    public final FilterParser.condition_comparison2_return condition_comparison2() throws RecognitionException {
        FilterParser.condition_comparison2_return retval = new FilterParser.condition_comparison2_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        FilterParser.column_spec_return column_spec43 =null;

        FilterParser.greatless_char_return greatless_char44 =null;

        FilterParser.digit_expr_return digit_expr45 =null;


        RewriteRuleSubtreeStream stream_column_spec=new RewriteRuleSubtreeStream(adaptor,"rule column_spec");
        RewriteRuleSubtreeStream stream_greatless_char=new RewriteRuleSubtreeStream(adaptor,"rule greatless_char");
        RewriteRuleSubtreeStream stream_digit_expr=new RewriteRuleSubtreeStream(adaptor,"rule digit_expr");
        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:133:2: ( column_spec greatless_char digit_expr -> ^( EXP_BOOL column_spec greatless_char digit_expr ) )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:133:4: column_spec greatless_char digit_expr
            {
            pushFollow(FOLLOW_column_spec_in_condition_comparison2612);
            column_spec43=column_spec();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_column_spec.add(column_spec43.getTree());

            pushFollow(FOLLOW_greatless_char_in_condition_comparison2614);
            greatless_char44=greatless_char();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_greatless_char.add(greatless_char44.getTree());

            pushFollow(FOLLOW_digit_expr_in_condition_comparison2616);
            digit_expr45=digit_expr();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) stream_digit_expr.add(digit_expr45.getTree());

            // AST REWRITE
            // elements: digit_expr, column_spec, greatless_char
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            if ( state.backtracking==0 ) {

            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 133:42: -> ^( EXP_BOOL column_spec greatless_char digit_expr )
            {
                // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:133:45: ^( EXP_BOOL column_spec greatless_char digit_expr )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(EXP_BOOL, "EXP_BOOL")
                , root_1);

                adaptor.addChild(root_1, stream_column_spec.nextTree());

                adaptor.addChild(root_1, stream_greatless_char.nextTree());

                adaptor.addChild(root_1, stream_digit_expr.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;
            }

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "condition_comparison2"


    public static class equal_char_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "equal_char"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:136:1: equal_char : ( EQ | NOT_EQ );
    public final FilterParser.equal_char_return equal_char() throws RecognitionException {
        FilterParser.equal_char_return retval = new FilterParser.equal_char_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token set46=null;

        CommonTree set46_tree=null;

        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:137:2: ( EQ | NOT_EQ )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:
            {
            root_0 = (CommonTree)adaptor.nil();


            set46=(Token)input.LT(1);

            if ( input.LA(1)==EQ||input.LA(1)==NOT_EQ ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, 
                (CommonTree)adaptor.create(set46)
                );
                state.errorRecovery=false;
                state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "equal_char"


    public static class greatless_char_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "greatless_char"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:140:1: greatless_char : ( GTH | GEQ | LTH | LEQ );
    public final FilterParser.greatless_char_return greatless_char() throws RecognitionException {
        FilterParser.greatless_char_return retval = new FilterParser.greatless_char_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token set47=null;

        CommonTree set47_tree=null;

        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:141:2: ( GTH | GEQ | LTH | LEQ )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:
            {
            root_0 = (CommonTree)adaptor.nil();


            set47=(Token)input.LT(1);

            if ( (input.LA(1) >= GEQ && input.LA(1) <= GTH)||input.LA(1)==LEQ||input.LA(1)==LTH ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, 
                (CommonTree)adaptor.create(set47)
                );
                state.errorRecovery=false;
                state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "greatless_char"


    public static class column_spec_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "column_spec"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:145:1: column_spec : filter_identifier ( DOT filter_identifier )* ;
    public final FilterParser.column_spec_return column_spec() throws RecognitionException {
        FilterParser.column_spec_return retval = new FilterParser.column_spec_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token DOT49=null;
        FilterParser.filter_identifier_return filter_identifier48 =null;

        FilterParser.filter_identifier_return filter_identifier50 =null;


        CommonTree DOT49_tree=null;

        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:146:2: ( filter_identifier ( DOT filter_identifier )* )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:146:4: filter_identifier ( DOT filter_identifier )*
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_filter_identifier_in_column_spec681);
            filter_identifier48=filter_identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, filter_identifier48.getTree());

            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:146:22: ( DOT filter_identifier )*
            loop9:
            do {
                int alt9=2;
                int LA9_0 = input.LA(1);

                if ( (LA9_0==DOT) ) {
                    alt9=1;
                }


                switch (alt9) {
            	case 1 :
            	    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:146:24: DOT filter_identifier
            	    {
            	    DOT49=(Token)match(input,DOT,FOLLOW_DOT_in_column_spec685); if (state.failed) return retval;
            	    if ( state.backtracking==0 ) {
            	    DOT49_tree = 
            	    (CommonTree)adaptor.create(DOT49)
            	    ;
            	    adaptor.addChild(root_0, DOT49_tree);
            	    }

            	    pushFollow(FOLLOW_filter_identifier_in_column_spec687);
            	    filter_identifier50=filter_identifier();

            	    state._fsp--;
            	    if (state.failed) return retval;
            	    if ( state.backtracking==0 ) adaptor.addChild(root_0, filter_identifier50.getTree());

            	    }
            	    break;

            	default :
            	    break loop9;
                }
            } while (true);


            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "column_spec"


    public static class column_name_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "column_name"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:149:1: column_name : filter_identifier ;
    public final FilterParser.column_name_return column_name() throws RecognitionException {
        FilterParser.column_name_return retval = new FilterParser.column_name_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        FilterParser.filter_identifier_return filter_identifier51 =null;



        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:150:2: ( filter_identifier )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:150:4: filter_identifier
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_filter_identifier_in_column_name701);
            filter_identifier51=filter_identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, filter_identifier51.getTree());

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "column_name"


    public static class identifier_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "identifier"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:155:1: identifier : ( ID | DOUBLEQUOTED_STRING );
    public final FilterParser.identifier_return identifier() throws RecognitionException {
        FilterParser.identifier_return retval = new FilterParser.identifier_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token set52=null;

        CommonTree set52_tree=null;

        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:156:2: ( ID | DOUBLEQUOTED_STRING )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:
            {
            root_0 = (CommonTree)adaptor.nil();


            set52=(Token)input.LT(1);

            if ( input.LA(1)==DOUBLEQUOTED_STRING||input.LA(1)==ID ) {
                input.consume();
                if ( state.backtracking==0 ) adaptor.addChild(root_0, 
                (CommonTree)adaptor.create(set52)
                );
                state.errorRecovery=false;
                state.failed=false;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return retval;}
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "identifier"


    public static class filter_identifier_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "filter_identifier"
    // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:161:1: filter_identifier : identifier ;
    public final FilterParser.filter_identifier_return filter_identifier() throws RecognitionException {
        FilterParser.filter_identifier_return retval = new FilterParser.filter_identifier_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        FilterParser.identifier_return identifier53 =null;



        try {
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:162:2: ( identifier )
            // G:\\MyStudy\\����\\antlr\\presentproject\\FilterParser.g:162:4: identifier
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_identifier_in_filter_identifier735);
            identifier53=identifier();

            state._fsp--;
            if (state.failed) return retval;
            if ( state.backtracking==0 ) adaptor.addChild(root_0, identifier53.getTree());

            }

            retval.stop = input.LT(-1);


            if ( state.backtracking==0 ) {

            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);
            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
    	retval.tree = (CommonTree)adaptor.errorNode(input, retval.start, input.LT(-1), re);

        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "filter_identifier"

    // Delegated rules


    protected DFA6 dfa6 = new DFA6(this);
    static final String DFA6_eotS =
        "\11\uffff";
    static final String DFA6_eofS =
        "\11\uffff";
    static final String DFA6_minS =
        "\1\11\1\10\1\11\2\uffff\1\33\2\uffff\1\10";
    static final String DFA6_maxS =
        "\1\26\1\52\1\26\2\uffff\1\35\2\uffff\1\52";
    static final String DFA6_acceptS =
        "\3\uffff\1\1\1\2\1\uffff\1\3\1\4\1\uffff";
    static final String DFA6_specialS =
        "\11\uffff}>";
    static final String[] DFA6_transitionS = {
            "\1\1\14\uffff\1\1",
            "\1\2\3\uffff\1\3\5\uffff\2\4\7\uffff\1\7\1\uffff\1\6\1\5\4"+
            "\uffff\1\4\1\uffff\1\4\4\uffff\1\3",
            "\1\10\14\uffff\1\10",
            "",
            "",
            "\1\7\1\uffff\1\6",
            "",
            "",
            "\1\2\3\uffff\1\3\5\uffff\2\4\7\uffff\1\7\1\uffff\1\6\1\5\4"+
            "\uffff\1\4\1\uffff\1\4\4\uffff\1\3"
    };

    static final short[] DFA6_eot = DFA.unpackEncodedString(DFA6_eotS);
    static final short[] DFA6_eof = DFA.unpackEncodedString(DFA6_eofS);
    static final char[] DFA6_min = DFA.unpackEncodedStringToUnsignedChars(DFA6_minS);
    static final char[] DFA6_max = DFA.unpackEncodedStringToUnsignedChars(DFA6_maxS);
    static final short[] DFA6_accept = DFA.unpackEncodedString(DFA6_acceptS);
    static final short[] DFA6_special = DFA.unpackEncodedString(DFA6_specialS);
    static final short[][] DFA6_transition;

    static {
        int numStates = DFA6_transitionS.length;
        DFA6_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA6_transition[i] = DFA.unpackEncodedString(DFA6_transitionS[i]);
        }
    }

    class DFA6 extends DFA {

        public DFA6(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 6;
            this.eot = DFA6_eot;
            this.eof = DFA6_eof;
            this.min = DFA6_min;
            this.max = DFA6_max;
            this.accept = DFA6_accept;
            this.special = DFA6_special;
            this.transition = DFA6_transition;
        }
        public String getDescription() {
            return "108:1: predicate : ( condition_comparison1 | condition_comparison2 | condition_like | condition_between );";
        }
    }
 

    public static final BitSet FOLLOW_stat_in_program180 = new BitSet(new long[]{0x0000001040400202L});
    public static final BitSet FOLLOW_filter_condition_in_stat193 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_filter_expression_in_filter_expressions203 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_expr_expr_in_filter_expression215 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_condition_or_in_filter_condition307 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_condition_and_in_condition_or319 = new BitSet(new long[]{0x0000000100000002L});
    public static final BitSet FOLLOW_KW_OR_in_condition_or323 = new BitSet(new long[]{0x0000001040400200L});
    public static final BitSet FOLLOW_condition_and_in_condition_or325 = new BitSet(new long[]{0x0000000100000002L});
    public static final BitSet FOLLOW_condition_factor_in_condition_and350 = new BitSet(new long[]{0x0000000004000002L});
    public static final BitSet FOLLOW_KW_AND_in_condition_and354 = new BitSet(new long[]{0x0000001040400200L});
    public static final BitSet FOLLOW_condition_factor_in_condition_and356 = new BitSet(new long[]{0x0000000004000002L});
    public static final BitSet FOLLOW_KW_NOT_in_condition_factor382 = new BitSet(new long[]{0x0000001000400200L});
    public static final BitSet FOLLOW_condition_expr_in_condition_factor384 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_condition_expr_in_condition_factor389 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_predicate_in_condition_expr410 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_boolean_predicand_in_condition_expr415 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_parenthesized_boolean_value_expression_in_boolean_predicand427 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LPAREN_in_parenthesized_boolean_value_expression444 = new BitSet(new long[]{0x0000001040400200L});
    public static final BitSet FOLLOW_condition_or_in_parenthesized_boolean_value_expression446 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_RPAREN_in_parenthesized_boolean_value_expression449 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_condition_comparison1_in_predicate466 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_condition_comparison2_in_predicate471 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_condition_like_in_predicate476 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_condition_between_in_predicate481 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_column_spec_in_condition_between493 = new BitSet(new long[]{0x0000000048000000L});
    public static final BitSet FOLLOW_between_predicate_part_2_in_condition_between495 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_NOT_in_between_predicate_part_2512 = new BitSet(new long[]{0x0000000008000000L});
    public static final BitSet FOLLOW_KW_BETWEEN_in_between_predicate_part_2517 = new BitSet(new long[]{0x0000400290810000L});
    public static final BitSet FOLLOW_filter_expression_in_between_predicate_part_2519 = new BitSet(new long[]{0x0000000004000000L});
    public static final BitSet FOLLOW_KW_AND_in_between_predicate_part_2522 = new BitSet(new long[]{0x0000400290810000L});
    public static final BitSet FOLLOW_filter_expression_in_between_predicate_part_2524 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_column_spec_in_condition_like546 = new BitSet(new long[]{0x0000000060000000L});
    public static final BitSet FOLLOW_KW_NOT_in_condition_like550 = new BitSet(new long[]{0x0000000020000000L});
    public static final BitSet FOLLOW_KW_LIKE_in_condition_like555 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_QUOTED_STRING_in_condition_like557 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_column_spec_in_condition_comparison1582 = new BitSet(new long[]{0x0000040000001000L});
    public static final BitSet FOLLOW_equal_char_in_condition_comparison1584 = new BitSet(new long[]{0x0000400290810000L});
    public static final BitSet FOLLOW_filter_expression_in_condition_comparison1586 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_column_spec_in_condition_comparison2612 = new BitSet(new long[]{0x00000028000C0000L});
    public static final BitSet FOLLOW_greatless_char_in_condition_comparison2614 = new BitSet(new long[]{0x0000000000810000L});
    public static final BitSet FOLLOW_digit_expr_in_condition_comparison2616 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_filter_identifier_in_column_spec681 = new BitSet(new long[]{0x0000000000000102L});
    public static final BitSet FOLLOW_DOT_in_column_spec685 = new BitSet(new long[]{0x0000000000400200L});
    public static final BitSet FOLLOW_filter_identifier_in_column_spec687 = new BitSet(new long[]{0x0000000000000102L});
    public static final BitSet FOLLOW_filter_identifier_in_column_name701 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_identifier_in_filter_identifier735 = new BitSet(new long[]{0x0000000000000002L});

}