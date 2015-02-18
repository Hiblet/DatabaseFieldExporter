using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Xml.Schema; // XMLSchemaSet
using System.Xml.Linq; // XDocument
using System.Xml; // XmlReader
using System.IO; // StringReader

namespace NZ01
{

    /// <summary>
    /// Class to validate the XMLLimitSpec documents.
    /// </summary>
    /// <remarks>
    /// Usage: 
    ///  - Instantiate an object with strings; 
    ///  - Call it's member SchemaValidate() with an XDocument argument.
    ///  - Alternatively, call SchemaValidateAndThrow() to generate exception on failure.
    /// </remarks>
    public class PositionLimitValidator
    {
        private XmlSchemaSet _xmlSchemaSet;

        public PositionLimitValidator(string sContractKey, string sSymbolKey, string sBuyKey, string sSellKey, string sPositionKey)
        {
            string xsd = createSchema();
            xsd = xsd.Replace("CONTRACT_KEY", sContractKey)
                .Replace("SYMBOL_KEY", sSymbolKey)
                .Replace("BUY_KEY", sBuyKey)
                .Replace("SELL_KEY", sSellKey)
                .Replace("POSITION_KEY", sPositionKey);

            _xmlSchemaSet = new XmlSchemaSet();
            _xmlSchemaSet.Add("", XmlReader.Create(new StringReader(xsd)));
        }

        public string SchemaValidate(XDocument doc)
        {
            string err = string.Empty;

            doc.Validate(_xmlSchemaSet, (o, e) =>
            {
                err += e.Message + "|";
            }, true);

            err.Trim('|');

            return err;
        }

        public void SchemaValidateAndThrow(XDocument doc)
        {
            string err = SchemaValidate(doc);

            if (!string.IsNullOrWhiteSpace(err))
            {
                throw new System.ArgumentException(string.Format("XMLLimitSpec document failed validation: {0}", err));
            }
        }

        private static string createSchema()
        {
            return @"<xsd:schema xmlns:xsd='http://www.w3.org/2001/XMLSchema'>
                  <xsd:element name='CONTRACT_KEY'>
                    <xsd:complexType>
                      <xsd:sequence>
                        <xsd:element ref='CONTRACT_KEY' minOccurs='0' maxOccurs='unbounded'/>
                      </xsd:sequence>
                      <xsd:attribute name='SYMBOL_KEY' use='required'>
                        <xsd:simpleType>
                          <xsd:restriction base='xsd:string'>
                            <xsd:whiteSpace value='collapse'/>
                          </xsd:restriction>
                        </xsd:simpleType>
                      </xsd:attribute>
                      <xsd:attribute name='BUY_KEY' use='required'>
                        <xsd:simpleType>
                          <xsd:restriction base='xsd:integer'>
                            <xsd:minInclusive value='-1'/>
                          </xsd:restriction>
                        </xsd:simpleType>
                      </xsd:attribute>
                      <xsd:attribute name='SELL_KEY' use='required'>
                        <xsd:simpleType>
                          <xsd:restriction base='xsd:integer'>
                            <xsd:minInclusive value='-1'/>
                          </xsd:restriction>
                        </xsd:simpleType>
                      </xsd:attribute>
                      <xsd:attribute name='POSITION_KEY' type='xsd:integer'/>
                    </xsd:complexType>
                  </xsd:element>
                </xsd:schema>";
        }

    }

} // end of namespace NZ01