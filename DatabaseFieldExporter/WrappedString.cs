using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NZ01
{
    /// <summary>
    /// A string  class to force case to upper in all circumstances, and allow case insensitive comparison
    /// </summary>
    public class StringUpper
    {
        // Instance Member
        private readonly string _value;


        // Ctor
        public StringUpper(string s)
        { _value = s.ToUpper(); }


        // Implicit Operators

        public static implicit operator string(StringUpper sc)
        { return sc.ToString(); }

        public static implicit operator StringUpper(string s)
        { return new StringUpper(s); }



        // Properties

        public string Value
        {
            get { return _value; }
            // ReadOnly, no setter
        }
 


        // Class Mechanics Overhead

        public override string ToString()
        { return _value; }

        public override bool Equals(object obj)
        { return (obj.ToString() == _value); }

        public override int GetHashCode()
        { return _value.GetHashCode(); }

        public static bool operator ==(StringUpper a, StringUpper b)
        { return a.Equals(b); }

        public static bool operator !=(StringUpper a, StringUpper b)
        { return !a.Equals(b); }

    } // end of class StringUpper





    public class StringLower 
    {
        // Instance Member
        private readonly string _value;


        // Ctor
        public StringLower(string s)
        { _value = s.ToLower(); }


        // Implicit Operators

        public static implicit operator string(StringLower sc)
        { return sc.ToString(); }

        public static implicit operator StringLower(string s)
        { return new StringLower(s); }



        // Properties

        public string Value
        {
            get { return _value; }
            // ReadOnly, no setter
        }


        // Class Mechanics Overhead

        public override string ToString()
        { return _value; }

        public override bool Equals(object obj)
        { return (obj.ToString() == _value); }

        public override int GetHashCode()
        { return _value.GetHashCode(); }

        public static bool operator ==(StringLower a, StringLower b)
        { return a.Equals(b); }

        public static bool operator !=(StringLower a, StringLower b)
        { return !a.Equals(b); }


    } // end of class StringLower


} // end of namespace NZ01
