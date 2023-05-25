package edu.upenn.cis.cis455.storage.berkeleydb;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.collections.StoredSortedEntrySet;
import com.sleepycat.collections.StoredSortedKeySet;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.collections.StoredSortedValueSet;
import com.sleepycat.collections.StoredValueSet;

class StorageViews {

    private StoredSortedMap<String, String> userMap;
    private StoredSortedMap<String, String> urlMd5Map;
    private StoredSortedMap<String, String> md5DocMap;
    
    public StorageViews(StorageDatabase db) {
        EntryBinding<String> StringBinding = new SerialBinding<String>(db.getClassCatalog(), String.class);
        // user
        userMap = new StoredSortedMap<String, String>(db.getUserDatabase(), StringBinding, StringBinding, true);
        // url to md5
        urlMd5Map = new StoredSortedMap<String, String>(db.getUrlMd5Database(), StringBinding, StringBinding, true);
        // md5 to doc
        md5DocMap = new StoredSortedMap<String, String>(db.getMd5DocDatabase(), StringBinding, StringBinding, true);
    }

    ///////////////////////////////////////////////////////////////////////////
    ////////////////////////////// USER-PASSWD ////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////

    public final StoredSortedMap<String, String> getUserMap() {
        return userMap;
    }

    public final StoredSortedKeySet<String> getUserKeys() {
        return (StoredSortedKeySet<String>) userMap.keySet();
    }

    public final StoredValueSet<String> getUserValues() {
        return (StoredValueSet<String>) userMap.values();
    }
    
    public final StoredSortedEntrySet<String, String> getUserEntries() {
        return (StoredSortedEntrySet<String, String>) userMap.entrySet();
    }

    ///////////////////////////////////////////////////////////////////////////
    ////////////////////////////// URL-MD5-DOC ////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////

    public final StoredSortedMap<String, String> getUrlMd5Map() {
        return urlMd5Map;
    }

    public final StoredSortedKeySet<String> getUrlMd5KeySet() {
        return (StoredSortedKeySet<String>) urlMd5Map.keySet();
    }

    public final StoredValueSet<String> getUrlMd5ValueSet() {
        return (StoredValueSet<String>) urlMd5Map.values();
    }

    public final StoredSortedEntrySet<String, String> getUrlMd5EntrySet() {
        return (StoredSortedEntrySet<String, String>) urlMd5Map.entrySet();
    }

    public final StoredSortedMap<String, String> getMd5DocMap() {
        return md5DocMap;
    }

    public final StoredSortedKeySet<String> getMd5DocKeySet() {
        return (StoredSortedKeySet<String>) md5DocMap.keySet();
    }

    public final StoredValueSet<String> getMd5DocValueSet() {
        return (StoredValueSet<String>) md5DocMap.values();
    }

    public final StoredSortedEntrySet<String, String> getMd5DocEntrySet() {
        return (StoredSortedEntrySet<String, String>) md5DocMap.entrySet();
    }

}
