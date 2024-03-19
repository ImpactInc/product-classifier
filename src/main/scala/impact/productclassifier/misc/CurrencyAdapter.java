/*
 *
 *  * Copyright (C) 2015 by Estalea, Inc. All rights reserved.
 *  * This is proprietary software of Estalea, Inc.
 *
 */

package impact.productclassifier.misc;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.Currency;


/**
 * Serializes and deserializes Currency objects like the old version of Gson did.
 * <p>
 * An alternative would be to serialize using the new improved way and cater for both formats when deserializing,
 * but then others relying on our document format would also need to make updates. See IRDW-987.
 */

public class CurrencyAdapter extends TypeAdapter<Currency> {
    
    @Override
    public Currency read(JsonReader in) throws IOException {
        in.beginObject();
        in.nextName();
        String currencyCode = in.nextString();
        in.endObject();
        
        return Currency.getInstance(currencyCode);
    }
    
    @Override
    public void write(JsonWriter out, Currency value) throws IOException {
        if (value != null) {
            out.beginObject().name("currencyCode").value(value.getCurrencyCode()).endObject();
        } else {
            out.nullValue();
        }
    }
}
