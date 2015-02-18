using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

using System.Xml.Linq; // XDocument, XElement, XAttribute
//using System.Xml.XPath; // XPath

namespace NZ01
{

    /// <summary>
    /// Class to represent a PositionLimit for a single account.
    /// </summary>
    /// <remarks>
    /// Each account has position limits stored in an XML document.  Because everyone loves XML.
    /// But also because the tree structure allows groups/hierachies.
    /// 
    /// Limit values are always positive ints, and there is a distinct buy and sell limit.
    /// Position values are positive or negative ints, depending on whether they are long positions (buy)
    /// or short positions (sell).
    /// 
    /// Symbols may be grouped, and a limit applied to the group. 
    /// Symbols may appear in more than one group.
    /// For example, the DAX June 2016 contract might be in a group of DAX symbols, and also
    /// in a group of June symbols.  If either limit is broken, the attempt to trade is blocked.
    /// 
    /// The XML tree is (at time of writing) self-symmetric, in that every element is similar.
    /// </remarks>
    public class PositionLimit
    {
        ///////////////////
        // STATIC MEMBERS

        private static log4net.ILog logger = log4net.LogManager.GetLogger("PositionLimit");



        //////////
        // ENUMS

        public enum DefaultLimitMgmt : long { NOT_RESTRICTED = -1, RESTRICTED = 0, MAX }

        ///////////////////
        // CONSTANTS

        // These settings dictate the text values used to access the XML elements.
        public const string ROOT_KEY = "ROOT";
        public const string BUY_KEY = "B";
        public const string SELL_KEY = "S";
        public const string POSITION_KEY = "P";
        public const string SYMBOL_KEY = "SYM";
        public const string CONTRACT_KEY = "CG";

        // Example element: <CG SYM="DAXJUN2014" B="-1" S="-1" P="45" />


        /////////////////////
        // INSTANCE MEMBERS

        private Int64 _accountKey;
        private Int64 _seqNum = 0;
        private StringUpper _virtualpath = "";
        private XDocument _limits;
        private string _lasterr = "";
        private string _failed = "";
        private DefaultLimitMgmt _restricted = DefaultLimitMgmt.RESTRICTED;



        //////////
        // CTORS

        /// <summary>
        /// Default Ctor
        /// </summary>
        /// <param name="accountKey"></param>
        /// <param name="restricted"></param>
        public PositionLimit(Int64 accountKey, bool restricted = true)
        {
            _accountKey = accountKey;
            _seqNum = 1;            
            _restricted = restricted ? DefaultLimitMgmt.RESTRICTED : DefaultLimitMgmt.NOT_RESTRICTED;
            constructXmlLimitSpec("");
        }


        public PositionLimit(Int64 accountKey, string xmlLimitSpec, string virtualPath, bool restricted)
        {
            _accountKey = accountKey;
            _seqNum = 1;
            _virtualpath = virtualPath;
            _restricted = restricted ? DefaultLimitMgmt.RESTRICTED : DefaultLimitMgmt.NOT_RESTRICTED;
            constructXmlLimitSpec(xmlLimitSpec); // Handles empty string case
        }

        /// <summary>
        /// Ctor from Database record;
        /// </summary>
        /// <param name="accountKey"></param>
        /// <param name="xmlLimitSpec"></param>
        /// <param name="defaultLimitMgmt"></param>
        /// <param name="seqNum"></param>
        public PositionLimit(Int64 accountKey, string xmlLimitSpec, string defaultLimitMgmt, string virtualPath, Int64 seqNum = 0)
        {
            _accountKey = accountKey;
            _seqNum = seqNum + 1; // Advance the sequence number 
            _virtualpath = virtualPath;
            _restricted = lookupDefaultLimitMgmtFromString(defaultLimitMgmt);
            constructXmlLimitSpec(xmlLimitSpec);
        }

        /// <summary>
        /// Helper for Limit Spec construction from string
        /// </summary>
        /// <param name="xmlLimitSpec">string; String representation of XmlLimitSpec XDocument</param>
        private void constructXmlLimitSpec(string xmlLimitSpec)
        {
            // Empty limit spec
            if (string.IsNullOrWhiteSpace(xmlLimitSpec))
                _limits = getNewLimitSpec();
            else
            {
                try
                {
                    _limits = XDocument.Parse(xmlLimitSpec);
                    PositionLimitValidator plv = new PositionLimitValidator(CONTRACT_KEY, SYMBOL_KEY, BUY_KEY, SELL_KEY, POSITION_KEY);
                    plv.SchemaValidateAndThrow(_limits);
                }
                catch (Exception ex)
                {
                    // Eat exception, but record it, and create blank risk doc
                    _lasterr = ex.Message;
                    _failed = xmlLimitSpec;
                    _limits = getNewLimitSpec();
                }
            }

            //synchronisePositions();
        }


        /// <summary>
        /// Copy Constructor, for DEEP COPY
        /// </summary>
        /// <param name="rhsPL">PositionLimit object to copy</param>
        /// <remarks>
        /// A Deep copy makes an independent copy of each member.
        /// The tricky part is the XDocument, but this class has a deep copy constructor,
        /// so an independent copy can be taken at this point.
        /// </remarks>
        public PositionLimit(PositionLimit rhsPL)
        {
            _accountKey = rhsPL._accountKey;
            _seqNum = rhsPL._seqNum;
            _virtualpath = rhsPL._virtualpath;
            _restricted = rhsPL._restricted;
            _lasterr = rhsPL._lasterr;
            _failed = rhsPL._failed;
            _limits = new XDocument(rhsPL._limits);
        }






        //////////////
        // ACCESSORS

        public Int64 AccountKey
        {
            get { return _accountKey; }
        }

        public Int64 SeqNum()
        {
            return _seqNum;
        }

        public string VirtualPath
        {
            get { return _virtualpath; }
            set { _virtualpath = value; }
        }

        public XDocument XmlLimitSpec
        {
            get { return _limits; }
            set
            {
                try
                {
                    PositionLimitValidator plv = new PositionLimitValidator(CONTRACT_KEY, SYMBOL_KEY, BUY_KEY, SELL_KEY, POSITION_KEY);
                    plv.SchemaValidateAndThrow(value);
                    _limits = value;
                    //synchronisePositions();
                }
                catch (Exception ex)
                {
                    // Eat exception, but record it, and keep the current document
                    _lasterr = ex.Message;
                    _failed = value.ToString();
                }
            }
        }

        public String GetLastError()
        {
            return _lasterr;
        }

        public void ResetLastError()
        {
            _lasterr = "";
        }

        public DefaultLimitMgmt DefaultRestrictionLimit
        {
            get { return _restricted; }
            set { _restricted = value; }
        }



        ///////////////
        // MEMBER FNS


        public bool HasLimit(string symbol)
        {
            //IEnumerable<XElement> elements = _limits.Descendants(symbol); // WAS

            IEnumerable<XElement> elements = getDescendants(_limits, SYMBOL_KEY, symbol);
            return elements.Any();
        }


        /// <summary>
        /// Get all elements from below the argument element that match on this attribute
        /// </summary>
        /// <param name="attribute">string; Attribute to select on</param>
        /// <param name="target">string; Target value of attribute</param>
        /// <returns></returns>
        private IEnumerable<XElement> getDescendants(XContainer element, string attribute, string target)
        {
            return element.Descendants().Where(e => evaluateMatch(e, attribute, target));

            // TO USE WILDCARD
            // return _limits.Descendants().Where(e => evaluateWildcardMatch(e, attribute, target));
        }

        /// <summary>
        /// Get child elements that match on an attribute
        /// </summary>
        /// <param name="parent">XContainer; Parent container, XElement or XDocument</param>
        /// <param name="attribute">string; Attribute</param>
        /// <param name="target">string; target attribute value</param>
        /// <returns></returns>
        private IEnumerable<XElement> getChildElements(XContainer parent, string attribute, string target)
        {
            return (parent.Elements().Where(e => evaluateMatch(e, attribute, target))).ToList();
        }

        private XElement getElementByXAttributePath(XElement parent, string attribute, string sXAttributePath, ref bool success)
        {
            XElement iterator = parent;

            // Break the path up by forward slash delimiter
            string[] array = sXAttributePath.Split('/');

            // Shortcut out if there are no elements returned at all
            if (!array.Any())
            {
                success = true;
                return iterator;
            }

            int i = 0;
            while (i < array.Length && (array[i] == ROOT_KEY || array[i] == "" || array[i] == "."))
            {
                ++i;
            }

            while (i < array.Length)
            {
                // Get Child with this attribute; If good, move on, if not, exit and return parent

                IEnumerable<XElement> childElements = getChildElements(iterator, attribute, array[i]);
                if (childElements.Count() == 1)
                {
                    iterator = childElements.First();
                }
                else
                {
                    success = false;
                    return parent;
                }

                ++i;
            }

            // success, reached the end of the array, successfully navigated to correct child
            success = true;
            return iterator;
        }


        private bool evaluateMatch(XElement element, string attribute, string target)
        {
            var attributeValue = element.Attribute(attribute).Value;

            if (attributeValue != null)
                return (attributeValue.ToString() == target);
            else
                return false;
        }

        /*
        private bool evaluateWildcardMatch(XElement element, string attribute, string target)
        {
            var attributeValue = element.Attribute(attribute).Value;

            if (attributeValue != null)
            {
                Wildcard wildcard = new Wildcard(attributeValue.ToString(), System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                return wildcard.IsMatch(target);
            }
            else
                return false;
        }
        */
        /*
        /// <summary>
        /// Retrieve all rules for a symbol, create if not existent.
        /// </summary>
        /// <param name="symbol">Symbol (Market)</param>
        /// <param name="limitCreated">bool flag, set true if rules did not already exist</param>
        /// <returns>
        /// A HashSet of type XElement; HashSet ensures no duplicates.
        /// </returns>
        private HashSet<XElement> getRelevantRules(string symbol, ref bool limitCreated)
        {
            HashSet<XElement> uniqueRules = new HashSet<XElement>(); // HashSet ensures no dupes

            // Get all the rules that contain the symbol
            //IEnumerable<XElement> rules = _limits.Descendants(symbol); // WAS
            IEnumerable<XElement> rules = getDescendants(_limits, SYMBOL_KEY, symbol);

            // If there is no limit rule in place, add one, and signal back that the spec changed
            if (!rules.Any())
            {
                // SetPositionInLimitSpec() will add the limit entry, update position and update the total
                // root position if there is currently a non-zero position in this symbol.

                SetPositionInLimitSpec(symbol, PositionLimitManager.GetPosition(_accountKey, symbol));

                // Now there should be an entry in the spec, we can re-pull it...
                //rules = _limits.Descendants(symbol); // WAS
                rules = getDescendants(_limits, SYMBOL_KEY, symbol);
                limitCreated = true;
            }

            // For each rule containing the symbol, get the one instance of the ancestors
            foreach (XElement el in rules)
            {
                IEnumerable<XElement> ancestorRules = el.AncestorsAndSelf();
                foreach (XElement ancestorEl in ancestorRules)
                {
                    uniqueRules.Add(ancestorEl);
                }
            }

            return uniqueRules;
        }
        */ 

        /// <summary>
        /// Check the proposed position change against existing rules.
        /// </summary>
        /// <param name="doc">XML Limit doc</param>
        /// <param name="symbol">Contract symbol</param>
        /// <param name="proposedDelta">The proposed position change (pos or neg)</param>
        /// <returns>A list of XElements that are limit rules that have been broken</returns>
        /// <remarks>An empty return list implies no broken rules, and the order has passed the checks</remarks>
        /*
        public IEnumerable<string> CheckRules(string symbol, Int64 proposedDelta, ref bool limitCreated)
        {
            List<XElement> brokenRules = new List<XElement>();

            if (proposedDelta == 0)
                return convertRulesToString(brokenRules); // Insanity shortcut

            HashSet<XElement> uniqueRules = getRelevantRules(symbol, ref limitCreated);

            // Check the proposed position delta against each rule, return any that are broken
            foreach (XElement el in uniqueRules)
            {
                Int64 currentPos = getAttributeValueFromXElement(el, POSITION_KEY);
                Int64 proposedPos = currentPos + proposedDelta;

                Int64 limit;

                if (proposedDelta > 0)
                {
                    // Limit value of -1 means 'ignore', or unrestricted
                    limit = getAttributeValueFromXElement(el, BUY_KEY);
                    if ((limit >= 0) && (proposedPos > limit))
                    {
                        brokenRules.Add(el);
                    }
                }
                else
                {
                    // Limit of -1 means ignore, so with negation, check that the limit is less than zero
                    limit = -1 * getAttributeValueFromXElement(el, SELL_KEY);
                    if ((limit <= 0) && (proposedPos < limit))
                    {
                        brokenRules.Add(el);
                    }
                }
            }

            return convertRulesToString(brokenRules);
        }
        */ 

        /// <summary>
        /// Retrieve the number of lots that can currently be traded within limits.
        /// </summary>
        /// <param name="symbol">Symbol (Market)</param>
        /// <param name="side">Side Enum, Side.BUY or Side.SELL</param>
        /// <returns>Int64 Quantity that can be traded on this side; Int64.MaxValue implies no limit</returns>
        /// <remarks>
        /// For example: 
        /// Assume limits of Buy=10, Sell=10, and a Position of +5 (bought 5).
        /// On the buy side, 5 lots can be bought without breaching the Buy=10 limit,
        /// so the function should return 5;
        /// On the sell side, 15 lots can be sold without breaching the Sell=10 limit,
        /// so the function should return 15;
        /// </remarks>
        /*
        public Int64 GetCapacity(string symbol, Side side, ref bool limitCreated)
        {
            var prefix = string.Format("GetCapacity(Symbol={0},Side={1}) - ", symbol, side);
            Int64 result = Int64.MaxValue;

            if (!(side == Side.BUY || side == Side.SELL))
            {
                logger.Error(prefix + "Limit check for an order with side that is neither BUY or SELL.");
                return result;
            }

            HashSet<XElement> uniqueRules = getRelevantRules(symbol, ref limitCreated);
            foreach (XElement el in uniqueRules)
            {
                Int64 currentPos = getAttributeValueFromXElement(el, POSITION_KEY);
                Int64 capacity = Int64.MaxValue;

                if (side == Side.BUY)
                {
                    Int64 limit = getAttributeValueFromXElement(el, BUY_KEY);

                    // A limit of "-1" implies no limit, thus capacity is not restricted
                    if (limit >= 0)
                        capacity = (limit - currentPos);

                }
                else
                {
                    Int64 limit = getAttributeValueFromXElement(el, SELL_KEY);

                    // A limit of "-1" implies no limit, thus capacity is not restricted
                    if (limit >= 0)
                        capacity = (currentPos + limit);
                }

                // Find the minimum capacity allowed
                if (capacity < result)
                    result = capacity;
            }

            return result;
        }
        */

        /// <summary>
        /// Write the current position into the Limits document, and update positions accordingly
        /// </summary>
        /// <param name="doc">XML Limit doc</param>
        /// <param name="symbol">Contract symbol</param>
        /// <param name="position">New or current position</param>
        /// <remarks>
        /// If there is no entry for the contract, one is created with no limits (-1).
        /// A limit value of -1 implies no restriction.
        /// </remarks>
        public void SetPositionInLimitSpec(string symbol, Int64 position)
        {
            //IEnumerable<XElement> elements = _limits.Root.Descendants(symbol); // WAS
            IEnumerable<XElement> elements = getDescendants(_limits, SYMBOL_KEY, symbol);

            Int64 delta = position;

            if (elements.Any())
            {
                // Work out the change in position
                Int64 prevPosition = getAttributeValueFromXElement(elements.First(), POSITION_KEY);
                delta = position - prevPosition;

                // Update positions for this symbol
                foreach (XElement el in elements)
                {
                    el.SetAttributeValue(POSITION_KEY, position);
                }
            }
            else
            {
                // Add an entry for this symbol

                //_limits.Root.Add(
                //    new XElement(
                //        symbol, 
                //        new XAttribute(BUY_KEY, (Int64)_restricted),
                //        new XAttribute(SELL_KEY, (Int64)_restricted), 
                //        new XAttribute(POSITION_KEY, position.ToString())));

                _limits.Root.Add(
                    new XElement(
                        CONTRACT_KEY,
                        new XAttribute(SYMBOL_KEY, symbol),
                        new XAttribute(BUY_KEY, (Int64)_restricted),
                        new XAttribute(SELL_KEY, (Int64)_restricted),
                        new XAttribute(POSITION_KEY, position.ToString())));

                elements = getDescendants(_limits, SYMBOL_KEY, symbol);
            }

            if (delta == 0) return; // Just in case there is no work to do, shortcut out.


            // Update all parents by the delta.  
            // Note that a contract may appear in more than one group, so effort must 
            // be taken to ensure that each parent element is updated only once.
            // To this end, a HashSet is used, as this ensures uniqueness.
            HashSet<XElement> elementsToUpdate = new HashSet<XElement>();
            foreach (XElement el in elements)
            {
                IEnumerable<XElement> ancestors = el.Ancestors();
                foreach (XElement ancestorEl in ancestors)
                {
                    elementsToUpdate.Add(ancestorEl);
                }
            }

            // Iterate the unique set and update positions by the delta value
            foreach (XElement el in elementsToUpdate)
            {
                Int64 currentPosition = getAttributeValueFromXElement(el, POSITION_KEY);
                el.SetAttributeValue(POSITION_KEY, currentPosition + delta);
            }
        }



        public bool SetPositionLimit(string symbol, Int64 limitBuy, Int64 limitSell)
        {
            // Wrapper for overloaded SetPositionLimit that defaults to the root as entry point.
            setPositionLimit(_limits.Root, symbol, limitBuy, limitSell);
            return true;
        }


        public bool SetPositionLimit(string sXAttributePath, string symbolOrGroup, Int64 limitBuy, Int64 limitSell)
        {
            var prefix = "SetPositionLimit() - ";

            XElement parent = _limits.Root;
            XContainer iterator = parent;

            // If changing limits at the root, pass the XDocument as the argument
            // else, try to find the parent element in the doc
            if (symbolOrGroup != PositionLimit.ROOT_KEY)
            {
                try
                {
                    bool success = false;
                    iterator = getElementByXAttributePath(parent, SYMBOL_KEY, sXAttributePath, ref success);
                    if (!success)
                        return false;
                }
                catch (Exception ex)
                {
                    logger.Warn(prefix + string.Format("Error occurred: {0}\r\nFailed to find XDocument/XElement from XAttributePath=>{1}<, for AccountKey=>{2}<, with Limits=\r\n{3}", ex.Message, sXAttributePath, _accountKey, _limits.ToString()));
                    return false;
                }
            }
            else
            {
                iterator = _limits;
            }

            setPositionLimit(iterator, symbolOrGroup, limitBuy, limitSell);
            return true;
        }


        /// <summary>
        /// Add a limit for a symbol, or group of symbols
        /// </summary>
        /// <param name="iterator">XElement location in tree to add node</param>
        /// <param name="symbolOrGroup">Contract symbol or name for group that will later contain symbols</param>
        /// <param name="limitBuy">Maximum Buy (Long) Position</param>
        /// <param name="limitSell">Maximum Sell (Short) Position</param>
        private void setPositionLimit(XContainer iterator, string symbolOrGroup, Int64 limitBuy, Int64 limitSell)
        {
            var prefix = string.Format("setPositionLimit(symbolOrGroup={0}) - ", symbolOrGroup);

            IEnumerable<XElement> childElements = getChildElements(iterator, SYMBOL_KEY, symbolOrGroup);

            int iCountChildElements = childElements.Count();

            if (iCountChildElements == 0)
            {
                // Does not exist, add at this parent
                iterator.Add(
                    new XElement(
                        CONTRACT_KEY,
                        new XAttribute(SYMBOL_KEY, symbolOrGroup),
                        new XAttribute(BUY_KEY, limitBuy),
                        new XAttribute(SELL_KEY, limitSell)));
            }
            else if (iCountChildElements == 1)
            {
                // There is one child element with this symbol; Update it
                XElement el = childElements.First();
                el.SetAttributeValue(BUY_KEY, limitBuy);
                el.SetAttributeValue(SELL_KEY, limitSell);
            }
            else
            {
                // Multiple child elements exist, and we have an error
                logger.Error(prefix + string.Format("Multiple entries exist for Symbol/Group {0} - This should never happen.", symbolOrGroup));
            }

        }

        /// <summary>
        /// Synchronise the limit spec with current known positions 
        /// </summary>
        /// 
        /*
        private void synchronisePositions()
        {
            // Reset all positions
            IEnumerable<XElement> elements = _limits.Root.DescendantsAndSelf(); // All The Things
            foreach (XElement el in elements)
            {
                el.SetAttributeValue(POSITION_KEY, 0);
            }


            // Get all the symbols referenced in the spec or that the account has a position for
            // and update the spec accordingly.
            HashSet<string> symbols = new HashSet<string>();

            // Symbols referenced in the spec
            foreach (XElement el in elements)
            {
                string symbol = string.Empty;
                var attributeValue = el.Attribute(SYMBOL_KEY).Value;
                if (attributeValue != null)
                    symbol = attributeValue.ToString();

                if (!string.IsNullOrWhiteSpace(symbol))
                {
                    symbols.Add(symbol);
                }
            }

            // Symbols with positions that may not be in the spec
            symbols.UnionWith(PositionLimitManager.GetSymbolsWithPositions(_accountKey));

            // Iterate the resultant set and set the position in the limit spec
            foreach (string uniqueSymbol in symbols)
            {
                Int64 pos = PositionLimitManager.GetPosition(_accountKey, uniqueSymbol);
                SetPositionInLimitSpec(uniqueSymbol, pos);
            }
        }
        */




        ////////////
        // HELPERS


        /// <summary>
        /// Convert the XElement rules to their string representation
        /// </summary>
        /// <param name="rules"></param>
        /// <returns></returns>
        /// <remarks>
        /// The thinking behind this is that it limits the extent of the usage of
        /// Linq to XML XDoc/XElement/XAttribute types to this class.
        /// </remarks>
        private IEnumerable<string> convertRulesToString(IEnumerable<XElement> rules)
        {
            List<string> stringRules = new List<string>();

            string sDoubleQuote = "\"";

            foreach (XElement rule in rules)
            {
                //string sRule = string.Format("Rule={0},{1},{2},{3}",
                //    rule.Path("-"),             
                //    rule.Attribute(BUY_KEY),
                //    rule.Attribute(SELL_KEY),
                //    rule.Attribute(POSITION_KEY));

                string sRule = string.Format("Rule={0},{1},{2},{3}",
                    rule.AttributePath(SYMBOL_KEY, "-"),
                    rule.Attribute(BUY_KEY),
                    rule.Attribute(SELL_KEY),
                    rule.Attribute(POSITION_KEY));

                // Note: Path delimiter default is "/", but this is removed by string.SqlSanitize() extension 
                //       method, so the default is over-ridden.

                // Remove double quotes, and rules that mean 'No Limit', as these are misleading noise.
                sRule = sRule.Replace(sDoubleQuote, string.Empty);
                sRule = sRule.Replace(",B=-1", string.Empty);
                sRule = sRule.Replace(",S=-1", string.Empty);

                stringRules.Add(sRule);
            }

            return stringRules;
        }

        /// <summary>
        /// Creates a virgin limit document with no limits set.
        /// </summary>
        /// <returns>XDocument object</returns>
        private XDocument getNewLimitSpec()
        {
            return new XDocument(
                new XElement(
                    CONTRACT_KEY,
                    new XAttribute(SYMBOL_KEY, ROOT_KEY),
                    new XAttribute(BUY_KEY, (Int64)_restricted),
                    new XAttribute(SELL_KEY, (Int64)_restricted)));
        }

        /// <summary>
        /// Get the Int64 value held in an XElement's attribute
        /// </summary>
        /// <param name="el">The XElement</param>
        /// <param name="attr">The name of the XElement's XAttribute</param>
        /// <returns>Int64 conversion of the string held in the attribute</returns>
        /// <remarks>
        /// If no attribute is present, should return zero.
        /// </remarks>
        private static Int64 getAttributeValueFromXElement(XElement el, string attr)
        {
            // If value of attribute is null, return 0
            Int64? value = (Int64?)el.Attribute(attr); // .Attribute returns attribute type;
            return (value == null ? 0 : (Int64)value);
        }




        ///////////////////
        // SQL FORMATTERS

        /* REMOVED: No longer required as xml spec should be written to file, not database
        private string sqlize(XDocument doc)
        {
            var prefix = "sqlize() - ";

            string sDoc = doc.ToString();

            // REMOVED: Limit spec is now file-backed so maximum size restriction no longer applies.
            //if (sDoc.Length > _MAXSIZE_XMLLIMITSPEC)
            //{
            //    string msgLimitSpecTooBig = 
            //        string.Format("The XML Limit Spec size ({0} chars) for AccountKey={1} is larger than the maximum database field size of {2}; The data will be truncated and corrupted and will have to be manually corrected.", sDoc.Length, _accountKey, _MAXSIZE_XMLLIMITSPEC);
            //    logger.Error(prefix + msgLimitSpecTooBig);
            //}

            return BaseDAL.sqlizeNoSanitize(doc.ToString(),_MAXSIZE_XMLLIMITSPEC,true);
        }
        */ 

        private string sqlize(DefaultLimitMgmt restricted)
        { return BaseDAL.sqlize(restricted.ToString()); }

        public string sqlAccountKey() { return _accountKey.ToString(); }
        //public string sqlXmlLimitSpec() { return sqlize(_limits); }
        public string sqlDefaultLimitMgmt() { return sqlize(_restricted); }



        /////////////////
        // ENUM LOOKUPS

        public static DefaultLimitMgmt lookupDefaultLimitMgmtFromString(string sDefaultLimitMgmt)
        {
            DefaultLimitMgmt restricted = DefaultLimitMgmt.RESTRICTED;

            if (sDefaultLimitMgmt == (DefaultLimitMgmt.NOT_RESTRICTED).ToString()) return DefaultLimitMgmt.NOT_RESTRICTED;

            return restricted;
        }



        /////////////////////////////
        // CLASS MECHANICS OVERHEAD

        public override string ToString()
        { return FC.FCGenerics.ToString<PositionLimit>(this); }

        //public new string ToStringStatics()                                 // Use new for all derived from base class
        //    { return FC.FCGenerics.ToString<Order>(this, true); }
        public virtual string ToStringStatics()                               // Use virtual for base class
        { return FC.FCGenerics.ToString<PositionLimit>(this, true); }

        public override bool Equals(object obj)
        { return (obj.ToString() == this.ToString()); }

        public override int GetHashCode()
        { return (this.ToString().GetHashCode()); }

    }

} // end of namespace NZ01