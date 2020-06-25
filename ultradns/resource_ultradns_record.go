package ultradns

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"regexp"

	"github.com/terra-farm/udnssdk"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

func newRRSetResource(d *schema.ResourceData) (rRSetResource, error) {
	r := rRSetResource{}

	// TODO: return error if required attributes aren't ok

	if attr, ok := d.GetOk("name"); ok {
		r.OwnerName = attr.(string)
	}

	if attr, ok := d.GetOk("type"); ok {
		r.RRType = attr.(string)
	}

	if attr, ok := d.GetOk("zone"); ok {
		r.Zone = attr.(string)
	}

	if attr, ok := d.GetOk("rdata"); ok {
		rdata := attr.(*schema.Set).List()
		r.RData = make([]string, len(rdata))
		for i, j := range rdata {
			r.RData[i] = j.(string)
		}
	}

	if attr, ok := d.GetOk("ttl"); ok {
		r.TTL, _ = strconv.Atoi(attr.(string))
	}

	return r, nil
}

func populateResourceDataFromRRSet(r udnssdk.RRSet, d *schema.ResourceData) error {
	zone := d.Get("zone")
	typ := d.Get("type")
	// ttl
	d.Set("ttl", r.TTL)
	// rdata
	rdata := r.RData

	// UltraDNS API returns answers double-encoded like JSON, so we must decode. This is their bug.
	if typ == "TXT" {
		rdata = make([]string, len(r.RData))
		for i := range r.RData {
			var s string
			err := json.Unmarshal([]byte(r.RData[i]), &s)
			if err != nil {
				log.Printf("[INFO] TXT answer parse error: %+v", err)
				s = r.RData[i]
			}
			rdata[i] = s

		}
	}

	err := d.Set("rdata", makeSetFromStrings(rdata))
	if err != nil {
		return fmt.Errorf("ultradns_record.rdata set failed: %#v", err)
	}
	// hostname
	if r.OwnerName == "" {
		d.Set("hostname", zone)
	} else {
		if strings.HasSuffix(r.OwnerName, ".") {
			d.Set("hostname", r.OwnerName)
		} else {
			d.Set("hostname", fmt.Sprintf("%s.%s", r.OwnerName, zone))
		}
	}
	return nil
}

func resourceUltradnsRecord() *schema.Resource {
	return &schema.Resource{
		Create: resourceUltraDNSRecordCreate,
		Read:   resourceUltraDNSRecordRead,
		Update: resourceUltraDNSRecordUpdate,
		Delete: resourceUltraDNSRecordDelete,
		Importer: &schema.ResourceImporter{
			State: resourceRecordImporter,
		},

		Schema: map[string]*schema.Schema{
			// Required
			"zone": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"type": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"rdata": {
				Type:     schema.TypeSet,
				Set:      schema.HashString,
				Required: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
			},
			// Optional
			"ttl": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "3600",
			},
			// Computed
			"hostname": {
				Type:     schema.TypeString,
				Computed: true,
			},
		},
	}
}

// CRUD Operations

func resourceUltraDNSRecordCreate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*udnssdk.Client)

	r, err := newRRSetResource(d)
	if err != nil {
		return err
	}

	log.Printf("[INFO] ultradns_record create: %+v", r)
	_, err = client.RRSets.Create(r.RRSetKey(), r.RRSet())
	if err != nil {
		return fmt.Errorf("create failed: %v", err)
	}

	d.SetId(r.ID())
	log.Printf("[INFO] ultradns_record.id: %v", d.Id())

	return resourceUltraDNSRecordRead(d, meta)
}

func resourceUltraDNSRecordRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*udnssdk.Client)
	r, err := newRRSetResource(d)

	if err != nil {
		return err
	}

	rrsets, err := client.RRSets.Select(r.RRSetKey())
	if err != nil {
		uderr, ok := err.(*udnssdk.ErrorResponseList)
		if ok {
			for _, r := range uderr.Responses {
				// 70002 means Records Not Found
				if r.ErrorCode == 70002 {
					d.SetId("")
					return nil
				}
				return fmt.Errorf("not found: %v", err)
			}
		}
		return fmt.Errorf("not found: %v", err)
	}
	rec := rrsets[0]
	return populateResourceDataFromRRSet(rec, d)
}

func resourceUltraDNSRecordUpdate(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*udnssdk.Client)

	r, err := newRRSetResource(d)
	if err != nil {
		return err
	}

	log.Printf("[INFO] ultradns_record update: %+v", r)
	_, err = client.RRSets.Update(r.RRSetKey(), r.RRSet())
	if err != nil {
		return fmt.Errorf("update failed: %v", err)
	}

	return resourceUltraDNSRecordRead(d, meta)
}

func resourceUltraDNSRecordDelete(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*udnssdk.Client)

	r, err := newRRSetResource(d)
	if err != nil {
		return err
	}

	log.Printf("[INFO] ultradns_record delete: %+v", r)
	_, err = client.RRSets.Delete(r.RRSetKey())
	if err != nil {
		return fmt.Errorf("delete failed: %v", err)
	}

	return nil
}

// resourceRecordImporter 
func resourceRecordImporter(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	value := d.Id()
	
	name, zone, err := resourceUltraDnsRecordParseId(value)

	if err != nil {
		return nil, err
	}

	d.Set("name", name)
	d.Set("zone", zone)
	return []*schema.ResourceData{d}, nil
}

// resourceUltraDnsRecordParseId takes an ID and parses it into its two constituent parts, which has two axes:
// a name and a zone. Since the separator character used in the Id may also exist in both the name and zone
// parts, e.g., name=app-name.domain.com and zone=domain.com, that combines to app-name.domain.com.domain.com
// we cannot simply split on the separator character and return the corresponding parts. Instead, we iterate
// through all the parts, reconstitute the parts to the left and right of the current index position, and test
// whether they match a regular expression used to evaluate DNS names. We evaluate ALL the parts, not just the 
// first one, so if no matches are found, or more than one match is found, we return an error since the Id 
// format cannot be safely parsed 
func resourceUltraDnsRecordParseId(id string) (string, string, error) {

	re := regexp.MustCompile(`^(([a-zA-Z]{1})|([a-zA-Z]{1}[a-zA-Z]{1})|([a-zA-Z]{1}[0-9]{1})|([0-9]{1}[a-zA-Z]{1})|([a-zA-Z0-9][a-zA-Z0-9-_]{1,61}[a-zA-Z0-9]))\.([a-zA-Z]{2,6}|[a-zA-Z0-9-]{2,30}\.[a-zA-Z
		]{2,3})$`)

	var	matches []potentialMatch

	items := strings.Split(id, ".")
	for i, _ := range items {

		// Create two mutually exclusive slices of the id segments 
		nameTokens := items[:i]
		zoneTokens := items[i:]

		// The slices must both have values for the contents to be a valid DNS
		// so exit if either of them does not
		if len(nameTokens) == 0 || len(zoneTokens) == 0 {
			continue
		}

		name := strings.Join(nameTokens, ".")
		zone := strings.Join(zoneTokens, ".")

		isNameValid := re.Match([]byte(name))
		isZoneValid := re.Match([]byte(zone))

		if (isNameValid && isZoneValid) {
			matches = append(matches, potentialMatch{zone: zone, name: name} )
		}
	}

	if matches == nil {
		return "", "", fmt.Errorf("Unexpected format of ID (%q), expected name.zone", id)
	}
	
	if len(matches) > 1 {
		return "", "", fmt.Errorf("Multiple segments of ID (%q) are valid DNS names. Cannot resolve and import.", id)
	}

	return matches[0].name, matches[0].zone, nil
}

// potentialMatch stores the zone and name values parsed from the 
// ultradns record resource id. Since the resource id may contain
// multiple valid DNS fragments, we can use this data structure
// to accumulate all the potential zone/name matches and proceed
// with import only when there is a SINGLE match
type potentialMatch struct {
	zone string
	name string
}
//Conversion helper functions