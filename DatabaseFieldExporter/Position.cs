using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace NZ01
{

    /// <summary>
    /// Class to represent a position for an Account, for a specific Symbol
    /// </summary>
    public class Position
    {
        ///////////////////
        // STATIC MEMBERS

        private static log4net.ILog logger = log4net.LogManager.GetLogger("MarketDefnDAL");


        /////////////////////
        // INSTANCE MEMBERS

        private Int64 _accountKey;
        private StringUpper _symbol;
        private string _marketName;
        private Int64 _position;
        private Int64 _seqNum;
        private string _reason; // reason for last position change



        public Position()
        {
            _position = 0;
            _seqNum = 0;
        }

        /*
        public Position(Int64 accountKey, string symbol)
        {
            _accountKey = accountKey;
            _symbol = symbol;
            _marketName = MarketManager.GetName(symbol);
            _position = 0;
            _seqNum = 0;
        }
        */
        /*
        public Position(Int64 accountKey, string symbol, Int64 position, string reason, Int64 seqNum = 0)
        {
            _accountKey = accountKey;
            _symbol = symbol;
            _marketName = MarketManager.GetName(symbol);
            _reason = reason;
            _position = position;
            _seqNum = seqNum + 1;
        }
        */

        // COPY CTOR
        public Position(Position rhsPosition)
        {
            if (rhsPosition == null)
            {
                _position = 0;
                _seqNum = 0;
                _reason = "COPY OF NULL ARGUMENT";
            }
            else
            {
                _accountKey = rhsPosition._accountKey;
                _symbol = rhsPosition._symbol;
                _marketName = rhsPosition._marketName;
                _reason = rhsPosition._reason;
                _position = rhsPosition._position;
                _seqNum = rhsPosition._seqNum;
            }
        }

        //////////////
        // ACCESSORS

        public Int64 AccountKey
        {
            get { return _accountKey; }
        }

        public string Symbol
        {
            get { return _symbol; }
        }

        public string MarketName
        {
            get { return _marketName; }
        }

        public Int64 Pos
        {
            get { return _position; }
            set
            {
                _position = value;
                ++_seqNum;
            }
        }

        public Int64 SeqNum
        {
            get { return _seqNum; }
            set { _seqNum = value; }
        }

        public string Reason
        {
            get { return _reason; }
            set { _reason = value; }
        }


        /////////////////////////////
        // CLASS MECHANICS OVERHEAD

        public override string ToString()
        { return FC.FCGenerics.ToString<Position>(this); }

        //public new string ToStringStatics()                                 // Use new for all derived from base class
        //    { return FC.FCGenerics.ToString<Order>(this, true); }
        public virtual string ToStringStatics()                               // Use virtual for base class
        { return FC.FCGenerics.ToString<Position>(this, true); }

        public override bool Equals(object obj)
        { return (obj.ToString() == this.ToString()); }

        public override int GetHashCode()
        { return (this.ToString().GetHashCode()); }

    }

} // end of namespace NZ01