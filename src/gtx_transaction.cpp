//
// Created by zhou822 on 6/1/23.
//
#include "../core/gtx_transaction.hpp"
#include "../core/edge_delta_block_state_protection.hpp"
#include "core/commit_manager.hpp"
#include <immintrin.h>
using namespace GTX;
//pessimistic mode
#if USING_PESSIMISTIC_MODE
//fixme:: we should not use this function. this is unchecked operation
Txn_Operation_Response RWTransaction::put_edge(vertex_t src, vertex_t dst, label_t label, std::string_view edge_data){
    //locate the vertex;
    BwLabelEntry* target_label_entry =writer_access_label(src,label);
    if(!target_label_entry){
        //todo:: this should not happen?
        // std::cout<<"should never happen"<<std::endl;
        return Txn_Operation_Response::FAIL;
    }
    //calculate block id
    uint64_t block_id = generate_block_id(src,label);
    //enter the block protection first
    if(BlockStateVersionProtectionScheme::writer_access_block(thread_id,block_id,target_label_entry,block_access_ts_table)){
        if(target_label_entry->block_ptr==0){
            throw GraphNullPointerException();
        }
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        //if the block is already overflow, return and wait
        if(current_block->already_overflow()){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return Txn_Operation_Response::WRITER_WAIT;
        }
        int32_t total_delta_chain_num = current_block->get_delta_chain_num();
        auto cached_delta_chain_access = per_block_cached_delta_chain_offsets.try_emplace(block_id, LockOffsetCache(target_label_entry->block_version_number,total_delta_chain_num));
        uint32_t current_delta_chain_head_offset = 0;
        //if there exits
        if(!cached_delta_chain_access.second){
            if(cached_delta_chain_access.first->second.is_outdated(target_label_entry->block_version_number.load(std::memory_order_acquire))){
                uint64_t current_block_offset = current_block->get_current_offset();
                if(current_block->is_overflow_offset(current_block_offset)){
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    return Txn_Operation_Response::WRITER_WAIT;
                }
#if LAZY_LOCKING
                bool reclaim_lock_offset_result = cached_delta_chain_access.first->second.reclaim_delta_chain_lock(current_block,target_label_entry,local_txn_id,read_timestamp,current_block_offset);
#else
                bool reclaim_lock_offset_result = cached_delta_chain_access.first->second.reclaim_delta_chain_lock(current_block,target_label_entry,local_txn_id,read_timestamp,current_block_offset,&lazy_update_records);
#endif
                if(!reclaim_lock_offset_result){
                    //need to abort: we can always safely use cache to abort
                    op_count -= cached_delta_chain_access.first->second.eager_abort(current_block,target_label_entry,local_txn_id,current_block_offset);
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    per_block_cached_delta_chain_offsets.erase(cached_delta_chain_access.first);
                    return Txn_Operation_Response::FAIL;
                }
            }
            //this step may create a 0 offset for a delta chain in the cache
            current_delta_chain_head_offset =  cached_delta_chain_access.first->second.ensure_delta_chain_cache(dst);//get the cached offset if there is a write already, otherwise stay at 0
        }
        auto* delta_chains_index = target_label_entry->delta_chain_index;
        const char* data = edge_data.data();
        delta_chain_id_t target_delta_chain_id = calculate_owner_delta_chain_id(dst,total_delta_chain_num);
        //indicate the current txn does not have the lock
        if(!current_delta_chain_head_offset){
            //todo: maybe also set protection on delta chain id?
            // auto lock_result = current_block->set_protection(dst,&lazy_update_records, read_timestamp);
#if LAZY_LOCKING
            auto lock_result = current_block->set_protection_on_delta_chain(target_delta_chain_id,&lazy_update_records,read_timestamp);
            if(lock_result == Delta_Chain_Lock_Response::CONFLICT){
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::FAIL; //abort on write-write conflict
            }
#else
            auto lock_result = current_block->simple_set_protection_on_delta_chain(target_delta_chain_id,&lazy_update_records,read_timestamp);
            if(lock_result==Delta_Chain_Lock_Response::CONFLICT){
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::FAIL; //abort on write-write conflict
            }else if(lock_result == Delta_Chain_Lock_Response::UNCLEAR){
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::WRITER_WAIT;
            }
#endif //LAZY_LOCKING
            current_delta_chain_head_offset = delta_chains_index->at(target_delta_chain_id).get_raw_offset();
            auto allocate_delta_result = allocate_delta(current_block, static_cast<int32_t>(edge_data.size()));
            if(allocate_delta_result==EdgeDeltaInstallResult::SUCCESS){
                //todo: maybe add an exist check? if exist, insert delta; otherwise update delta
                current_block->append_edge_delta(dst,local_txn_id,EdgeDeltaType::UPDATE_DELTA, data,static_cast<int32_t>(edge_data.size()),current_delta_chain_head_offset,current_delta_offset,current_data_offset);
                cached_delta_chain_access.first->second.cache_vid_offset_new(dst,current_delta_offset);
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                op_count++;
                return Txn_Operation_Response::SUCCESS;
            }else if (allocate_delta_result==EdgeDeltaInstallResult::ALREADY_OVERFLOW){
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::WRITER_WAIT;
            }else{//I caused overflow
                consolidation(target_label_entry,current_block, block_id);
                return put_edge(src,dst,label,edge_data);
            }
        }else{//the current transaction already locks the delta chain
            //todo: check if this part can be merged together with the previous part
            auto allocate_delta_result = allocate_delta(current_block,static_cast<int32_t>(edge_data.size()));
            if(allocate_delta_result==EdgeDeltaInstallResult::SUCCESS){
                current_block->append_edge_delta(dst,local_txn_id,EdgeDeltaType::UPDATE_DELTA,data,static_cast<int32_t>(edge_data.size()),current_delta_chain_head_offset,current_delta_offset,current_data_offset);
                cached_delta_chain_access.first->second.cache_vid_offset_exist(dst,current_delta_offset);
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                op_count++;
                return Txn_Operation_Response::SUCCESS;
            }else if(allocate_delta_result==EdgeDeltaInstallResult::ALREADY_OVERFLOW){
                //do not release lock
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::WRITER_WAIT;
            }else{
                consolidation(target_label_entry, current_block, block_id);
                return put_edge(src,dst,label,edge_data);
            }
        }
    }else{
        return Txn_Operation_Response::WRITER_WAIT;
    }
}
/*
 * create an edge delta as either insert or update
 */
Txn_Operation_Response RWTransaction::checked_put_edge(GTX::vertex_t src, GTX::vertex_t dst,
                                                       GTX::label_t label, std::string_view edge_data) {
    BwLabelEntry* target_label_entry =writer_access_label(src,label);
    if(!target_label_entry)[[unlikely]]{
        //todo:: this should not happen?
        // std::cout<<"should never happen"<<std::endl;
        return Txn_Operation_Response::FAIL;
    }
    //calculate block id
    uint64_t block_id = generate_block_id(src,label);
    if(BlockStateVersionProtectionScheme::writer_access_block(thread_id,block_id,target_label_entry,block_access_ts_table))[[likely]]{
        if(target_label_entry->block_ptr==0)[[unlikely]]{
            throw GraphNullPointerException();
        }
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        //if the block is already overflow, return and wait
        if(current_block->already_overflow())[[unlikely]]{
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return Txn_Operation_Response::WRITER_WAIT;
        }
        int32_t total_delta_chain_num = current_block->get_delta_chain_num();
        auto cached_delta_chain_access = per_block_cached_delta_chain_offsets.try_emplace(block_id, LockOffsetCache(target_label_entry->block_version_number,total_delta_chain_num));
        uint32_t current_delta_chain_head_offset = 0;
        //if there exits
        if(!cached_delta_chain_access.second){
            if(cached_delta_chain_access.first->second.is_outdated(target_label_entry->block_version_number.load())){
                uint64_t current_block_offset = current_block->get_current_offset();
                if(current_block->is_overflow_offset(current_block_offset)){
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    return Txn_Operation_Response::WRITER_WAIT;
                }
#if LAZY_LOCKING
                bool reclaim_lock_offset_result = cached_delta_chain_access.first->second.reclaim_delta_chain_lock(current_block,target_label_entry,local_txn_id,read_timestamp,current_block_offset);
#else
                bool reclaim_lock_offset_result = cached_delta_chain_access.first->second.reclaim_delta_chain_lock(current_block,target_label_entry,local_txn_id,read_timestamp,current_block_offset,&lazy_update_records);
#endif
                if(!reclaim_lock_offset_result){
                    //need to abort: we can always safely use cache to abort
                    op_count -= cached_delta_chain_access.first->second.eager_abort(current_block,target_label_entry,local_txn_id,current_block_offset);
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    per_block_cached_delta_chain_offsets.erase(cached_delta_chain_access.first);
                    return Txn_Operation_Response::FAIL;
                }
            }
            //this step may create a 0 offset for a delta chain in the cache
            current_delta_chain_head_offset =  cached_delta_chain_access.first->second.ensure_delta_chain_cache(dst);//get the cached offset if there is a write already, otherwise stay at 0
        }
        //auto* delta_chains_index = target_label_entry->delta_chain_index;
        const char* data = edge_data.data();
        delta_chain_id_t target_delta_chain_id = calculate_owner_delta_chain_id(dst,total_delta_chain_num);
        //indicate the current txn does not have the lock
        if(!current_delta_chain_head_offset){
            //todo: maybe also set protection on delta chain id?
#if LAZY_LOCKING
            auto lock_result = current_block->set_protection_on_delta_chain(target_delta_chain_id,&lazy_update_records,read_timestamp);
            if(lock_result == Delta_Chain_Lock_Response::CONFLICT){
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::FAIL; //abort on write-write conflict
            }
#else
            auto lock_result = current_block->simple_set_protection_on_delta_chain(target_delta_chain_id,&lazy_update_records,read_timestamp,&current_delta_chain_head_offset);
            if(lock_result==Delta_Chain_Lock_Response::CONFLICT)[[unlikely]]{
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::FAIL; //abort on write-write conflict
            }else if(lock_result == Delta_Chain_Lock_Response::UNCLEAR)[[unlikely]]{
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::WRITER_WAIT;
            }
#endif //LAZY_LOCKING
            //lookup previous version:
            auto allocate_delta_result = allocate_delta(current_block, static_cast<int32_t>(edge_data.size()));
            if(allocate_delta_result==EdgeDeltaInstallResult::SUCCESS)[[likely]]{
                uint32_t previous_version_offset=0;
                if(current_delta_chain_head_offset){
#if USING_SIMPLE_PREVIOUS_VERSION
                    previous_version_offset = current_block->fetch_previous_version_offset_simple(dst,current_delta_chain_head_offset,local_txn_id);
#else
                    previous_version_offset = current_block->fetch_previous_version_offset(dst,current_delta_chain_head_offset,local_txn_id,lazy_update_records);
#endif
                }
                //insert should be more common than update
                if(!previous_version_offset){
                    current_block->checked_append_edge_delta(dst,local_txn_id, EdgeDeltaType::INSERT_DELTA,data,static_cast<int32_t>(edge_data.size()),current_delta_chain_head_offset,previous_version_offset,current_delta_offset,current_data_offset);
                }else{
                    current_block->checked_append_edge_delta(dst,local_txn_id, EdgeDeltaType::UPDATE_DELTA,data,static_cast<int32_t>(edge_data.size()),current_delta_chain_head_offset,previous_version_offset,current_delta_offset,current_data_offset);
                }
                cached_delta_chain_access.first->second.cache_vid_offset_new(dst,current_delta_offset);
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                op_count++;
                if(!previous_version_offset){
                    return Txn_Operation_Response::SUCCESS_NEW_DELTA;
                }else{
#if USING_EAGER_CONSOLIDATION
                    graph.increment_thread_local_update_count();
                    cache_updated_block_id_and_version(block_id,target_label_entry->block_version_number.load());
#endif
                    return Txn_Operation_Response::SUCCESS_EXISTING_DELTA;
                }
            }else if (allocate_delta_result==EdgeDeltaInstallResult::ALREADY_OVERFLOW){
                //release the protection, there is a chance consolidation is not happening in the end.
                current_block->release_protection_delta_chain(target_delta_chain_id);
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::WRITER_WAIT;
            }else{//I caused overflow
                checked_consolidation(target_label_entry,current_block, block_id);
                return checked_put_edge(src,dst,label,edge_data);
            }
        }else{//the current transaction already locks the delta chain
            //todo: check if this part can be merged together with the previous part
            auto allocate_delta_result = allocate_delta(current_block,static_cast<int32_t>(edge_data.size()));
            if(allocate_delta_result==EdgeDeltaInstallResult::SUCCESS)[[likely]]{
#if USING_SIMPLE_PREVIOUS_VERSION
                uint32_t previous_version_offset = current_block->fetch_previous_version_offset_simple(dst,current_delta_chain_head_offset,local_txn_id);
#else
                uint32_t previous_version_offset = current_block->fetch_previous_version_offset(dst,current_delta_chain_head_offset,local_txn_id,lazy_update_records);
#endif
                if(!previous_version_offset){
                    current_block->checked_append_edge_delta(dst,local_txn_id, EdgeDeltaType::INSERT_DELTA,data,static_cast<int32_t>(edge_data.size()),current_delta_chain_head_offset,previous_version_offset,current_delta_offset,current_data_offset);
                }else{
                    current_block->checked_append_edge_delta(dst,local_txn_id, EdgeDeltaType::UPDATE_DELTA,data,static_cast<int32_t>(edge_data.size()),current_delta_chain_head_offset,previous_version_offset,current_delta_offset,current_data_offset);
                }
                cached_delta_chain_access.first->second.cache_vid_offset_exist(dst,current_delta_offset);
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                op_count++;
                if(!previous_version_offset){
                    return Txn_Operation_Response::SUCCESS_NEW_DELTA;
                }else{
#if USING_EAGER_CONSOLIDATION
                    graph.increment_thread_local_update_count();
                    cache_updated_block_id_and_version(block_id,target_label_entry->block_version_number.load());
#endif
                    return Txn_Operation_Response::SUCCESS_EXISTING_DELTA;
                }
            }else if(allocate_delta_result==EdgeDeltaInstallResult::ALREADY_OVERFLOW){
                //do not release lock, even if the consolidation did not take place.
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::WRITER_WAIT;
            }else{
                checked_consolidation(target_label_entry, current_block, block_id);
                return checked_put_edge(src,dst,label,edge_data);
            }
        }
    }else{
#if ENABLE_VERTEX_DELETION
        auto current_state = target_label_entry->state.load(std::memory_order_acquire);
        if(current_state==EdgeDeltaBlockState::DELETION)[[unlikely]]{
            return Txn_Operation_Response::FAIL;
        }
#endif
        return Txn_Operation_Response::WRITER_WAIT;
    }
}

Txn_Operation_Response RWTransaction::checked_single_put_edge(vertex_t src, vertex_t dst, label_t label, std::string_view edge_data){
    BwLabelEntry* target_label_entry =writer_access_label(src,label);
    if(!target_label_entry)[[unlikely]]{
        //todo:: this should not happen?
        // std::cout<<"should never happen"<<std::endl;
        return Txn_Operation_Response::FAIL;
    }
    //calculate block id
    uint64_t block_id = generate_block_id(src,label);
    if(BlockStateVersionProtectionScheme::writer_access_block(thread_id,block_id,target_label_entry,block_access_ts_table))[[likely]]{
        if(target_label_entry->block_ptr==0)[[unlikely]]{
            throw GraphNullPointerException();
        }
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        //if the block is already overflow, return and wait
        if(current_block->already_overflow())[[unlikely]]{
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return Txn_Operation_Response::WRITER_WAIT;
        }
        int32_t total_delta_chain_num = current_block->get_delta_chain_num();

        uint32_t current_delta_chain_head_offset = 0;
        //auto* delta_chains_index = target_label_entry->delta_chain_index;
        const char* data = edge_data.data();
        delta_chain_id_t target_delta_chain_id = calculate_owner_delta_chain_id(dst,total_delta_chain_num);
        //indicate the current txn does not have the lock

        //todo: maybe also set protection on delta chain id?
#if LAZY_LOCKING
        auto lock_result = current_block->set_protection_on_delta_chain(target_delta_chain_id,&lazy_update_records,read_timestamp);
            if(lock_result == Delta_Chain_Lock_Response::CONFLICT){
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::FAIL; //abort on write-write conflict
            }
#else
        auto lock_result = current_block->simple_set_protection_on_delta_chain(target_delta_chain_id,&lazy_update_records,read_timestamp,&current_delta_chain_head_offset);
        if(lock_result==Delta_Chain_Lock_Response::CONFLICT)[[unlikely]]{
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return Txn_Operation_Response::FAIL; //abort on write-write conflict
        }else if(lock_result == Delta_Chain_Lock_Response::UNCLEAR)[[unlikely]]{
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return Txn_Operation_Response::WRITER_WAIT;
        }
#endif //LAZY_LOCKING
        //lookup previous version:
        auto allocate_delta_result = allocate_delta(current_block, static_cast<int32_t>(edge_data.size()));
        if(allocate_delta_result==EdgeDeltaInstallResult::SUCCESS)[[likely]]{
            uint32_t previous_version_offset=0;
            if(current_delta_chain_head_offset){
#if USING_SIMPLE_PREVIOUS_VERSION
                previous_version_offset = current_block->fetch_previous_version_offset_simple(dst,current_delta_chain_head_offset,local_txn_id);
#else
                previous_version_offset = current_block->fetch_previous_version_offset(dst,current_delta_chain_head_offset,local_txn_id,lazy_update_records);
#endif
            }
            //insert should be more common than update
            if(!previous_version_offset){
                current_block->checked_append_edge_delta(dst,local_txn_id, EdgeDeltaType::INSERT_DELTA,data,static_cast<int32_t>(edge_data.size()),current_delta_chain_head_offset,previous_version_offset,current_delta_offset,current_data_offset);
            }else{
                current_block->checked_append_edge_delta(dst,local_txn_id, EdgeDeltaType::UPDATE_DELTA,data,static_cast<int32_t>(edge_data.size()),current_delta_chain_head_offset,previous_version_offset,current_delta_offset,current_data_offset);
            }
            auto cached_delta_chain_access = per_block_cached_delta_chain_offsets.try_emplace(block_id, LockOffsetCache(target_label_entry->block_version_number,total_delta_chain_num));
            cached_delta_chain_access.first->second.cache_vid_offset_new(dst,current_delta_offset);
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            op_count++;
            if(!previous_version_offset){
                return Txn_Operation_Response::SUCCESS_NEW_DELTA;
            }else{
#if USING_EAGER_CONSOLIDATION
                graph.increment_thread_local_update_count();
                    cache_updated_block_id_and_version(block_id,target_label_entry->block_version_number.load());
#endif
                return Txn_Operation_Response::SUCCESS_EXISTING_DELTA;
            }
        }else if (allocate_delta_result==EdgeDeltaInstallResult::ALREADY_OVERFLOW){
            //release the protection, there is a chance consolidation is not happening in the end.
            current_block->release_protection_delta_chain(target_delta_chain_id);
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return Txn_Operation_Response::WRITER_WAIT;
        }else{//I caused overflow
            checked_consolidation(target_label_entry,current_block, block_id);
            return checked_single_put_edge(src,dst,label,edge_data);
        }

    }else{
#if ENABLE_VERTEX_DELETION
        auto current_state = target_label_entry->state.load(std::memory_order_acquire);
        if(current_state==EdgeDeltaBlockState::DELETION)[[unlikely]]{
            return Txn_Operation_Response::FAIL;
        }
#endif
        return Txn_Operation_Response::WRITER_WAIT;
    }
}

//fixme:: unchecked functions should not be called
Txn_Operation_Response
RWTransaction::delete_edge(GTX::vertex_t src, GTX::vertex_t dst, GTX::label_t label) {
    BwLabelEntry* target_label_entry =writer_access_label(src,label);
    if(!target_label_entry){
        return Txn_Operation_Response::FAIL;
    }
    //calculate block id
    uint64_t block_id = generate_block_id(src,label);
    //enter the block protection first
    if(BlockStateVersionProtectionScheme::writer_access_block(thread_id,block_id,target_label_entry,block_access_ts_table)){
        if(target_label_entry->block_ptr==0){
            throw GraphNullPointerException();
        }
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        //if the block is already overflow, return and wait
        if(current_block->already_overflow()){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return Txn_Operation_Response::WRITER_WAIT;
        }
        int32_t total_delta_chain_num = current_block->get_delta_chain_num();
        auto cached_delta_chain_access = per_block_cached_delta_chain_offsets.try_emplace(block_id, LockOffsetCache(target_label_entry->block_version_number,total_delta_chain_num));
        uint32_t current_delta_chain_head_offset = 0;
        //if there exits
        if(!cached_delta_chain_access.second){
            if(cached_delta_chain_access.first->second.is_outdated(target_label_entry->block_version_number.load(std::memory_order_acquire))){
                uint64_t current_block_offset = current_block->get_current_offset();
                if(current_block->is_overflow_offset(current_block_offset)){
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    return Txn_Operation_Response::WRITER_WAIT;
                }
#if LAZY_LOCKING
                bool reclaim_lock_offset_result = cached_delta_chain_access.first->second.reclaim_delta_chain_lock(current_block,target_label_entry,local_txn_id,read_timestamp,current_block_offset);
#else
                bool reclaim_lock_offset_result = cached_delta_chain_access.first->second.reclaim_delta_chain_lock(current_block,target_label_entry,local_txn_id,read_timestamp,current_block_offset,&lazy_update_records);
#endif
                if(!reclaim_lock_offset_result){
                    //need to abort: we can always safely use cache to abort
                    op_count -= cached_delta_chain_access.first->second.eager_abort(current_block,target_label_entry,local_txn_id,current_block_offset);
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    per_block_cached_delta_chain_offsets.erase(cached_delta_chain_access.first);
                    return Txn_Operation_Response::FAIL;
                }
            }
            //this step may create a 0 offset for a delta chain in the cache
            current_delta_chain_head_offset =  cached_delta_chain_access.first->second.ensure_delta_chain_cache(dst);//get the cached offset if there is a write already, otherwise stay at 0
        }
        auto* delta_chains_index = target_label_entry->delta_chain_index;
        delta_chain_id_t target_delta_chain_id = calculate_owner_delta_chain_id(dst,total_delta_chain_num);
        //indicate the current txn does not have the lock
        if(!current_delta_chain_head_offset){
            //todo: maybe also set protection on delta chain id?
            // auto lock_result = current_block->set_protection(dst,&lazy_update_records, read_timestamp);
#if LAZY_LOCKING
            auto lock_result = current_block->set_protection_on_delta_chain(target_delta_chain_id,&lazy_update_records,read_timestamp);
            if(lock_result == Delta_Chain_Lock_Response::CONFLICT){
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::FAIL; //abort on write-write conflict
            }
#else
            auto lock_result = current_block->simple_set_protection_on_delta_chain(target_delta_chain_id,&lazy_update_records,read_timestamp);
            if(lock_result==Delta_Chain_Lock_Response::CONFLICT){
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::FAIL; //abort on write-write conflict
            }else if(lock_result == Delta_Chain_Lock_Response::UNCLEAR){
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::WRITER_WAIT;
            }
#endif //LAZY_LOCKING
            current_delta_chain_head_offset = delta_chains_index->at(target_delta_chain_id).get_raw_offset();
            auto allocate_delta_result = allocate_delta(current_block, 0);
            if(allocate_delta_result==EdgeDeltaInstallResult::SUCCESS){
                //todo: maybe add an exist check? if exist, insert delta; otherwise update delta
                current_block->append_edge_delta(dst,local_txn_id,EdgeDeltaType::DELETE_DELTA, nullptr,0,current_delta_chain_head_offset,current_delta_offset,current_data_offset);
                cached_delta_chain_access.first->second.cache_vid_offset_new(dst,current_delta_offset);
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                op_count++;
                return Txn_Operation_Response::SUCCESS;
            }else if (allocate_delta_result==EdgeDeltaInstallResult::ALREADY_OVERFLOW){
                // current_block->release_protection_delta_chain(target_delta_chain_id);// actually no need to release the lock at all
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::WRITER_WAIT;
            }else{//I caused overflow
                //current_block->release_protection_delta_chain(target_delta_chain_id);
                consolidation(target_label_entry,current_block, block_id);
                return delete_edge(src,dst,label);
            }
        }else{//the current transaction already locks the delta chain
            //todo: check if this part can be merged together with the previous part
            auto allocate_delta_result = allocate_delta(current_block,0);
            if(allocate_delta_result==EdgeDeltaInstallResult::SUCCESS){
                current_block->append_edge_delta(dst,local_txn_id,EdgeDeltaType::DELETE_DELTA, nullptr,0,current_delta_chain_head_offset,current_delta_offset,current_data_offset);
                cached_delta_chain_access.first->second.cache_vid_offset_exist(dst,current_delta_offset);
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                op_count++;
                return Txn_Operation_Response::SUCCESS;
            }else if(allocate_delta_result==EdgeDeltaInstallResult::ALREADY_OVERFLOW){
                //do not release lock
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::WRITER_WAIT;
            }else{
                consolidation(target_label_entry, current_block, block_id);
                return delete_edge(src,dst,label);
            }
        }
    }else{
        return Txn_Operation_Response::WRITER_WAIT;
    }
}

/*
 * create an edge deletion delta if the edge exists. Otherwise do nothing.
 */
Txn_Operation_Response
RWTransaction::checked_delete_edge(GTX::vertex_t src, GTX::vertex_t dst, GTX::label_t label) {
    BwLabelEntry* target_label_entry =writer_access_label(src,label);
    if(!target_label_entry){
        return Txn_Operation_Response::FAIL;
    }
    //calculate block id
    uint64_t block_id = generate_block_id(src,label);
    //enter the block protection first
    if(BlockStateVersionProtectionScheme::writer_access_block(thread_id,block_id,target_label_entry,block_access_ts_table))[[likely]]{
        if(target_label_entry->block_ptr==0)[[unlikely]]{
            throw GraphNullPointerException();
        }
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        //if the block is already overflow, return and wait
        if(current_block->already_overflow())[[unlikely]]{
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return Txn_Operation_Response::WRITER_WAIT;
        }
        int32_t total_delta_chain_num = current_block->get_delta_chain_num();
        auto cached_delta_chain_access = per_block_cached_delta_chain_offsets.try_emplace(block_id, LockOffsetCache(target_label_entry->block_version_number,total_delta_chain_num));
        uint32_t current_delta_chain_head_offset = 0;
        //if there exits
        if(!cached_delta_chain_access.second){
            if(cached_delta_chain_access.first->second.is_outdated(target_label_entry->block_version_number.load(std::memory_order_acquire))){
                uint64_t current_block_offset = current_block->get_current_offset();
                if(current_block->is_overflow_offset(current_block_offset)){
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    return Txn_Operation_Response::WRITER_WAIT;
                }
#if LAZY_LOCKING
                bool reclaim_lock_offset_result = cached_delta_chain_access.first->second.reclaim_delta_chain_lock(current_block,target_label_entry,local_txn_id,read_timestamp,current_block_offset);
#else
                bool reclaim_lock_offset_result = cached_delta_chain_access.first->second.reclaim_delta_chain_lock(current_block,target_label_entry,local_txn_id,read_timestamp,current_block_offset,&lazy_update_records);
#endif
                if(!reclaim_lock_offset_result){
                    //need to abort: we can always safely use cache to abort
                    op_count -= cached_delta_chain_access.first->second.eager_abort(current_block,target_label_entry,local_txn_id,current_block_offset);
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    per_block_cached_delta_chain_offsets.erase(cached_delta_chain_access.first);
                    return Txn_Operation_Response::FAIL;
                }
            }
            //this step may create a 0 offset for a delta chain in the cache
            current_delta_chain_head_offset =  cached_delta_chain_access.first->second.ensure_delta_chain_cache(dst);//get the cached offset if there is a write already, otherwise stay at 0
        }
        //auto* delta_chains_index = target_label_entry->delta_chain_index;
        delta_chain_id_t target_delta_chain_id = calculate_owner_delta_chain_id(dst,total_delta_chain_num);
        //indicate the current txn does not have the lock
        if(!current_delta_chain_head_offset){
            //todo: maybe also set protection on delta chain id?
            // auto lock_result = current_block->set_protection(dst,&lazy_update_records, read_timestamp);
#if LAZY_LOCKING
            auto lock_result = current_block->set_protection_on_delta_chain(target_delta_chain_id,&lazy_update_records,read_timestamp);
            if(lock_result == Delta_Chain_Lock_Response::CONFLICT){
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::FAIL; //abort on write-write conflict
            }
#else
            auto lock_result = current_block->simple_set_protection_on_delta_chain(target_delta_chain_id,&lazy_update_records,read_timestamp,&current_delta_chain_head_offset);
            if(lock_result==Delta_Chain_Lock_Response::CONFLICT)[[unlikely]]{
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::FAIL; //abort on write-write conflict
            }else if(lock_result == Delta_Chain_Lock_Response::UNCLEAR)[[unlikely]]{
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::WRITER_WAIT;
            }
#endif //LAZY_LOCKING
            //current_delta_chain_head_offset = delta_chains_index->at(target_delta_chain_id).get_raw_offset();
            auto allocate_delta_result = allocate_delta(current_block, 0);
            if(allocate_delta_result==EdgeDeltaInstallResult::SUCCESS)[[likely]]{
                uint32_t previous_version_offset = 0;
                if(current_delta_chain_head_offset){
#if USING_SIMPLE_PREVIOUS_VERSION
                    previous_version_offset = current_block->fetch_previous_version_offset_simple(dst,current_delta_chain_head_offset,local_txn_id);
#else
                    previous_version_offset = current_block->fetch_previous_version_offset(dst,current_delta_chain_head_offset,local_txn_id,lazy_update_records);
#endif
                }
                if(previous_version_offset){
                    current_block->checked_append_edge_delta(dst,local_txn_id,EdgeDeltaType::DELETE_DELTA, nullptr,0,current_delta_chain_head_offset,previous_version_offset, current_delta_offset,current_data_offset);
                    cached_delta_chain_access.first->second.cache_vid_offset_new(dst,current_delta_offset);
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    op_count++;
#if USING_EAGER_CONSOLIDATION
                    graph.increment_thread_local_update_count();
                    cache_updated_block_id_and_version(block_id,target_label_entry->block_version_number.load());
#endif

                    return Txn_Operation_Response::SUCCESS_EXISTING_DELTA;
                }else{
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    return Txn_Operation_Response::SUCCESS_NEW_DELTA;//indicate a delete is not needed
                }

            }else if (allocate_delta_result==EdgeDeltaInstallResult::ALREADY_OVERFLOW){
                //need to release the lock due to eager consolidation
                current_block->release_protection_delta_chain(target_delta_chain_id);
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::WRITER_WAIT;
            }else{//I caused overflow
                checked_consolidation(target_label_entry,current_block, block_id);
                return checked_delete_edge(src,dst,label);
            }
        }else{//the current transaction already locks the delta chain
            //todo: check if this part can be merged together with the previous part
            auto allocate_delta_result = allocate_delta(current_block,0);
            if(allocate_delta_result==EdgeDeltaInstallResult::SUCCESS)[[likely]]{
                //uint32_t previous_version_offset = current_block->fetch_previous_version_offset(dst,current_delta_chain_head_offset,local_txn_id,lazy_update_records);
#if USING_SIMPLE_PREVIOUS_VERSION
                uint32_t previous_version_offset = current_block->fetch_previous_version_offset_simple(dst,current_delta_chain_head_offset,local_txn_id);
#else
                uint32_t previous_version_offset = current_block->fetch_previous_version_offset(dst,current_delta_chain_head_offset,local_txn_id,lazy_update_records);
#endif
                if(previous_version_offset){
                    current_block->checked_append_edge_delta(dst,local_txn_id,EdgeDeltaType::DELETE_DELTA, nullptr,0,current_delta_chain_head_offset,previous_version_offset,current_delta_offset,current_data_offset);
                    cached_delta_chain_access.first->second.cache_vid_offset_exist(dst,current_delta_offset);
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    op_count++;
#if USING_EAGER_CONSOLIDATION
                    graph.increment_thread_local_update_count();
                    cache_updated_block_id_and_version(block_id, target_label_entry->block_version_number.load());
#endif
                    return Txn_Operation_Response::SUCCESS_EXISTING_DELTA;
                }else{
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    return Txn_Operation_Response::SUCCESS_NEW_DELTA;//indicate a delete is not needed
                }
            }else if(allocate_delta_result==EdgeDeltaInstallResult::ALREADY_OVERFLOW){
                //do not release lock
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::WRITER_WAIT;
            }else{
                checked_consolidation(target_label_entry, current_block, block_id);
                return checked_delete_edge(src,dst,label);
            }
        }
    }else{
#if ENABLE_VERTEX_DELETION
        auto current_state = target_label_entry->state.load(std::memory_order_acquire);
        if(current_state==EdgeDeltaBlockState::DELETION)[[unlikely]]{
            return Txn_Operation_Response::FAIL;
        }
#endif
        return Txn_Operation_Response::WRITER_WAIT;
    }
}



//fixme:: unchecked functions should not be called
void RWTransaction::consolidation(GTX::BwLabelEntry *current_label_entry, EdgeDeltaBlockHeader* current_block, uint64_t block_id) {
    //std::cout<<"consolidation starts"<<std::endl;
    BlockStateVersionProtectionScheme::install_exclusive_state(EdgeDeltaBlockState::OVERFLOW,thread_id,block_id,current_label_entry,block_access_ts_table);
    uint32_t original_delta_offset = current_delta_offset-ENTRY_DELTA_SIZE;
    uint32_t original_data_offset = current_data_offset;
    //also calculate approximately concurrent write-size
    uint64_t current_block_offset = current_block->get_current_offset();
    uint32_t overflow_data_size = (uint32_t)(current_block_offset>>32)-original_data_offset;
    uint32_t overflow_delta_size = (uint32_t)(current_block_offset&SIZE2MASK)-original_delta_offset;

    uint64_t to_restore_offset = combine_offset(original_delta_offset, original_data_offset);
#if CONSOLIDATION_TEST
    if(to_restore_offset<0x0000000100000000){
        throw std::runtime_error("overflow offsets");
    }
#endif
    current_block->set_offset(to_restore_offset);
    BlockStateVersionProtectionScheme::install_shared_state(EdgeDeltaBlockState::CONSOLIDATION,current_label_entry);
    std::unordered_set<vertex_t> edge_latest_versions_records;
    std::vector<uint32_t> edge_latest_version_offsets;
    BaseEdgeDelta* current_delta = current_block->get_edge_delta(original_delta_offset);
    std::unordered_map<uint64_t, std::vector<uint32_t>>in_progress_delta_per_txn;
    timestamp_t largest_invalidation_ts = std::numeric_limits<uint64_t>::min();//most recent previous version
    timestamp_t largest_creation_ts = std::numeric_limits<uint64_t>::min();//most recent (committed) version
    size_t data_size = 0;
    int32_t current_delta_chain_num = current_block->get_delta_chain_num();
    std::set<delta_chain_id_t> to_check_delta_chains;
    while(original_delta_offset>0){
        //should there be no invalid deltas
        timestamp_t original_ts = current_delta->creation_ts.load();
        if(!original_ts){
            throw std::runtime_error("all writer transactions should already installed their deltas");
        }
        //do lazy update if possible
        if(is_txn_id(original_ts)){
            uint64_t status = 0;
            if(txn_tables.get_status(original_ts,status)){
                if(status == IN_PROGRESS){
                    //do nothing
                }else{
                    if(status!=ABORT){
                        current_block->update_previous_delta_invalidate_ts(current_delta->toID, current_delta->previous_offset, status);
                        if(current_delta->lazy_update(original_ts,status)){
                            //record lazy update
                            record_lazy_update_record(&lazy_update_records, original_ts);
                        }

                    }
                    //if status == abort, must already be eager aborted
#if EDGE_DELTA_TEST
                    if(current_delta->creation_ts.load()!=status){
                        throw LazyUpdateException();
                    }
#endif
                }
            }
        }
        original_ts = current_delta->creation_ts.load();
        //now check the status
        if(is_txn_id(original_ts)){
            auto in_progress_txn_deltas_emplace_result = in_progress_delta_per_txn.try_emplace(original_ts, std::vector<uint32_t>());
            in_progress_txn_deltas_emplace_result.first->second.emplace_back(original_delta_offset);
            data_size+=current_delta->data_length+ENTRY_DELTA_SIZE;
            to_check_delta_chains.emplace(calculate_owner_delta_chain_id(current_delta->toID, current_delta_chain_num));
        }else if(original_ts!=ABORT){
            //skip committed delete deltas
            if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                vertex_t toID = current_delta->toID;
                auto latest_version_emplace_result = edge_latest_versions_records.emplace(toID);
                //if indeed latest version
                if(latest_version_emplace_result.second){
                    edge_latest_version_offsets.emplace_back(original_delta_offset);
                    largest_creation_ts = (largest_creation_ts>=original_ts)? largest_creation_ts:original_ts;
                    data_size+=current_delta->data_length+ENTRY_DELTA_SIZE;
                }else{
                    if(!current_delta->invalidate_ts){
                        throw LazyUpdateException();
                    }
                    largest_invalidation_ts = (largest_invalidation_ts>=current_delta->invalidate_ts.load())? largest_invalidation_ts:current_delta->invalidate_ts.load();
                }
            }
        }else{
            //do nothing
        }
        original_delta_offset-=ENTRY_DELTA_SIZE;
        current_delta++;
    }
    //handle edge case that the initial block was too small
    data_size = (data_size==0)? ENTRY_DELTA_SIZE:data_size;
    data_size+=overflow_data_size+overflow_delta_size;
    //analyze scan finished, now apply heuristics
    //use the block creation time vs. latest committed write to estimate lifespan
    uint64_t lifespan = largest_creation_ts - current_block->get_consolidation_time(); /*current_label_entry->consolidation_time;*/ //approximate lifespan of the block
    //todo:; apply different heuristics
    size_t new_block_size = calculate_nw_block_size_from_lifespan(data_size,lifespan,20);
    auto new_order = size_to_order(new_block_size);
    auto new_block_ptr = block_manager.alloc(new_order);
    auto new_block = block_manager.convert<EdgeDeltaBlockHeader>(new_block_ptr);
    //for debug
    /*  if(largest_invalidation_ts){
          std::cout<<"found"<<std::endl;
      }*/
    new_block->fill_metadata(current_block->get_owner_id(),largest_invalidation_ts, read_timestamp, current_label_entry->block_ptr,new_order, &txn_tables,current_label_entry->delta_chain_index);
    int32_t new_block_delta_chain_num = new_block->get_delta_chain_num();
    std::vector<AtomicDeltaOffset> new_delta_chains_index(new_block_delta_chain_num);
    //start installing latest version
    int64_t signed_edge_last_version_size = static_cast<int64_t>(edge_latest_version_offsets.size());
    for(int64_t i = (signed_edge_last_version_size-1); i>=0; i--){
        current_delta = current_block->get_edge_delta(edge_latest_version_offsets.at(i));
        if(is_txn_id(current_delta->creation_ts.load())){
            throw ConsolidationException();//latest versions should all be committed deltas
        }
        //find which delta chain the latest version delta belongs to
        delta_chain_id_t target_delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
        const char* data = current_block->get_edge_data(current_delta->data_offset);
        auto& new_delta_chains_index_entry = new_delta_chains_index.at(target_delta_chain_id);
        uint32_t new_block_delta_offset = new_delta_chains_index_entry.get_offset();//if cannot be locked
        auto consolidation_append_result = new_block->append_edge_delta(current_delta->toID,current_delta->creation_ts.load(),current_delta->delta_type,data,current_delta->data_length,new_block_delta_offset);
        if(consolidation_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!consolidation_append_result.second){
            throw ConsolidationException();
        }
        new_delta_chains_index_entry.update_offset(consolidation_append_result.second);
    }
    //we install all committed latest versions and form delta chains
    //now we start the exclusive installation phase
    BlockStateVersionProtectionScheme::install_exclusive_state(EdgeDeltaBlockState::INSTALLATION,thread_id,block_id,current_label_entry,block_access_ts_table);
    //wait for all validating transactions to finish
    while(true){
        for(auto it = to_check_delta_chains.begin(); it!= to_check_delta_chains.end();){
            //get the delta chain head of this delta chain in the original block, check if a validation is happening
            current_delta = current_block->get_edge_delta(current_label_entry->delta_chain_index->at(*it).get_raw_offset());
            uint64_t original_ts = current_delta->creation_ts.load();
            if(is_txn_id(original_ts)){
                uint64_t status =0;
                if(txn_tables.get_status(original_ts,status)){
                    //this transaction still validating, so we will come back later
                    if(status == IN_PROGRESS){
                        it++;
                        continue;
                    }else if(status ==ABORT){ //validating txn aborted, so consolidating thread will help with lazy update
                        uint32_t to_abort_offset = current_delta->previous_offset;
                        if(!current_delta->lazy_update(original_ts,status)){
                            throw ConsolidationException();//we should never fail lazy update during an exclusive state
                        }
                        auto abort_lazy_update_emplace_result = lazy_update_records.try_emplace(original_ts,1);
                        if(!abort_lazy_update_emplace_result.second){
                            abort_lazy_update_emplace_result.first->second++;
                        }
                        while(to_abort_offset!=0){
                            current_delta = current_block->get_edge_delta(to_abort_offset);
                            if(current_delta->creation_ts!=original_ts){
                                break;
                            }else if(current_delta->creation_ts==original_ts){
                                if(current_delta->lazy_update(original_ts,status)){
                                    abort_lazy_update_emplace_result.first->second++;
                                }else{
                                    throw ConsolidationException();
                                }
                            }
#if CONSOLIDATION_TEST
                            else if(current_delta->creation_ts==ABORT){
                                throw EagerAbortException();//aborted delta should not exit in the delta chain (except my own deltas that I'm aborting )
                            }
#endif
                            to_abort_offset = current_delta->previous_offset;
                        }
                        it = to_check_delta_chains.erase(it);//this delta chain is cleaned
                    }else{//original_ts txn committed after passing validation phase
                        uint32_t to_commit_offset = current_delta->previous_offset;
                        if(!current_delta->lazy_update(original_ts, status)){
                            throw ConsolidationException();
                        }
                        auto commit_lazy_update_emplace_result = lazy_update_records.try_emplace(original_ts,1);
                        if(!commit_lazy_update_emplace_result.second){
                            commit_lazy_update_emplace_result.first->second++;
                        }
                        while(to_commit_offset!=0){
                            current_delta= current_block->get_edge_delta(to_commit_offset);
                            if(current_delta->creation_ts!=original_ts&&current_delta->creation_ts!=status){//someone can already lazy update parts of txn's deltas due to concurrent readers
#if EDGE_DELTA_TEST
                                if(current_delta->creation_ts==ABORT){
                                    throw LazyUpdateException();
                                }
#endif
                                break;
                            }else if(current_delta->creation_ts==original_ts){
                                if(!current_delta->lazy_update(original_ts,status)){
                                    throw ConsolidationException();
                                }
                                commit_lazy_update_emplace_result.first->second++;
                            }
                            to_commit_offset = current_delta->previous_offset;
                        }
                        it = to_check_delta_chains.erase(it);
                    }
                }else{
                    throw ConsolidationException();//Installation phase is supposed to be exclusive.
                }
            }else{
                //this delta chain is not under validating or someone already lazy updated for it
                it = to_check_delta_chains.erase(it);
            }
        }
        if(to_check_delta_chains.empty()){
            break;
        }
    }
    //now check all in progress deltas recorded earlier
    //need 2 for loops because committed deltas should be before in-progress deltas
    for(auto it = in_progress_delta_per_txn.begin();it!=in_progress_delta_per_txn.end();){
        //uint64_t status;
        auto& all_delta_offsets_of_txn = it->second;
        current_delta = current_block->get_edge_delta(all_delta_offsets_of_txn.at(0));
        //skip them, will be installed at the end.
        if(is_txn_id(current_delta->creation_ts.load())){
            it++;
        }else if(current_delta->creation_ts.load()==ABORT){
            it = in_progress_delta_per_txn.erase(it);
        }else{//committed deltas
            timestamp_t commit_ts = current_delta->creation_ts.load();
            int64_t txn_own_deltas_size = static_cast<int64_t>(all_delta_offsets_of_txn.size());
            for(int64_t i = txn_own_deltas_size-1; i>=0; i--){
                uint64_t txn_id = it->first;
                current_delta= current_block->get_edge_delta(all_delta_offsets_of_txn.at(i));
                if(current_delta->lazy_update(txn_id,commit_ts)){
                    //todo:: delete this
                    std::cout<<"consolidation did not fully lazy update earlier"<<std::endl;
                    record_lazy_update_record(&lazy_update_records,txn_id);
                }
                //only install non-delete deltas todo:: may still need to install committed delete_delta, just in case it has a previous version?
                if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                    const char* data = current_block->get_edge_data(current_delta->data_offset);
                    delta_chain_id_t new_delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
                    auto& new_delta_chains_index_entry = new_delta_chains_index.at(new_delta_chain_id);
                    //can get raw offset because no lock yet
                    auto commit_delta_append_result = new_block->append_edge_delta(current_delta->toID,commit_ts,current_delta->delta_type,data,current_delta->data_length,new_delta_chains_index_entry.get_offset());
                    //can be a newer version:
                    new_block->update_previous_delta_invalidate_ts(current_delta->toID,new_delta_chains_index_entry.get_offset(),commit_ts);
                    if(commit_delta_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!commit_delta_append_result.second){
                        throw ConsolidationException();
                    }
                    new_delta_chains_index_entry.update_offset(commit_delta_append_result.second);
                }else{
                    //still install delete delta. At the worst it is a tombstone
                    delta_chain_id_t new_delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
                    auto& new_delta_chains_index_entry = new_delta_chains_index.at(new_delta_chain_id);
                    //can get raw offset because no lock yet
                    auto commit_delta_append_result = new_block->append_edge_delta(current_delta->toID,commit_ts,current_delta->delta_type,
                                                                                   nullptr,current_delta->data_length,new_delta_chains_index_entry.get_offset());
                    //can be a newer version:
                    new_block->update_previous_delta_invalidate_ts(current_delta->toID,new_delta_chains_index_entry.get_offset(),commit_ts);
                    if(commit_delta_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!commit_delta_append_result.second){
                        throw ConsolidationException();
                    }
                    new_delta_chains_index_entry.update_offset(commit_delta_append_result.second);
                }
            }
            it = in_progress_delta_per_txn.erase(it);
        }
    }
    //finally install those still in-progress deltas per txn
    for(auto it = in_progress_delta_per_txn.begin(); it!=in_progress_delta_per_txn.end();it++){
        uint64_t txn_id = it->first;
#if CONSOLIDATION_TEST
        uint64_t status;
        if(txn_tables.get_status(txn_id, status)){
            if(status!=IN_PROGRESS){
                throw ConsolidationException();
            }
        }else{
            throw ConsolidationException();
        }
#endif
        auto& txn_in_progress_deltas = it->second;
        std::vector<uint32_t>local_delta_chains_index_cache(new_block_delta_chain_num);
        for(int32_t j=0; j<new_block_delta_chain_num;j++){
            local_delta_chains_index_cache[j]=new_delta_chains_index[j].get_offset();
        }
        int64_t txn_delta_num = static_cast<int64_t>(txn_in_progress_deltas.size());
        for(int64_t i =txn_delta_num-1; i>=0; i--){
            current_delta = current_block->get_edge_delta(txn_in_progress_deltas.at(i));
#if CONSOLIDATION_TEST
            if(current_delta->creation_ts.load()!=txn_id){
                throw ConsolidationException();
            }
#endif
            const char* data = current_block->get_edge_data(current_delta->data_offset);
            delta_chain_id_t delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
            auto in_progress_delta_append_result = new_block->append_edge_delta(current_delta->toID, txn_id, current_delta->delta_type, data, current_delta->data_length, local_delta_chains_index_cache[delta_chain_id]);
#if CONSOLIDATION_TEST
            if(in_progress_delta_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!in_progress_delta_append_result.second){
                throw ConsolidationException();
            }
#endif
            local_delta_chains_index_cache[delta_chain_id]= in_progress_delta_append_result.second;
        }
    }
    //now consolidation is over
    per_thread_garbage_queue.register_entry(current_label_entry->block_ptr,current_block->get_order(),largest_invalidation_ts);
#if !USING_COMMIT_WAIT_WORK
    if(per_thread_garbage_queue.need_collection()){
        auto safe_ts = block_access_ts_table.calculate_safe_ts();
        per_thread_garbage_queue.free_block(safe_ts);
    }
#endif//USING_COMMIT_WAIT_WORK
    *current_label_entry->delta_chain_index = std::move(new_delta_chains_index);//todo::check its correctness
    current_label_entry->block_ptr = new_block_ptr;
/*    if(new_block->already_overflow()){
        throw ConsolidationException();
    }*/
    current_label_entry->block_version_number.fetch_add(1);//increase version by 1 /*consolidation_time.store(commit_manager.get_current_read_ts());*/
    BlockStateVersionProtectionScheme::install_shared_state(EdgeDeltaBlockState::NORMAL,current_label_entry);
    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
}

/*
 * consolidate the overflow block, and allocate a new block version.
 */
void RWTransaction::checked_consolidation(GTX::BwLabelEntry *current_label_entry,
                                          GTX::EdgeDeltaBlockHeader *current_block, uint64_t block_id) {
    BlockStateVersionProtectionScheme::install_exclusive_state(EdgeDeltaBlockState::OVERFLOW,thread_id,block_id,current_label_entry,block_access_ts_table);
    uint32_t original_delta_offset = current_delta_offset-ENTRY_DELTA_SIZE;
    uint32_t original_data_offset = current_data_offset;
    //also calculate approximately concurrent write-size
    uint64_t current_block_offset = current_block->get_current_offset();
    uint32_t overflow_data_size = (uint32_t)(current_block_offset>>32)-original_data_offset;
    uint32_t overflow_delta_size = (uint32_t)(current_block_offset&SIZE2MASK)-original_delta_offset;

    uint64_t to_restore_offset = combine_offset(original_delta_offset, original_data_offset);
#if CONSOLIDATION_TEST
    if(to_restore_offset<0x0000000100000000){
        throw std::runtime_error("overflow offsets");
    }
#endif
    current_block->set_offset(to_restore_offset);
    BlockStateVersionProtectionScheme::install_shared_state(EdgeDeltaBlockState::CONSOLIDATION,current_label_entry);
    std::unordered_set<vertex_t> edge_latest_versions_records;
    std::vector<uint32_t> edge_latest_version_offsets;
    BaseEdgeDelta* current_delta = current_block->get_edge_delta(original_delta_offset);
    std::unordered_map<uint64_t, std::vector<uint32_t>>in_progress_delta_per_txn;
    timestamp_t largest_invalidation_ts = std::numeric_limits<uint64_t>::min();//most recent previous version
    timestamp_t largest_creation_ts = std::numeric_limits<uint64_t>::min();//most recent (committed) version
    size_t data_size = 0;
    int32_t current_delta_chain_num = current_block->get_delta_chain_num();
    std::set<delta_chain_id_t> to_check_delta_chains;
    while(original_delta_offset>0){
        //should there be no invalid deltas
        timestamp_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
        if(!original_ts){
            throw std::runtime_error("all writer transactions should already installed their deltas");
        }
        //do lazy update if possible
        if(is_txn_id(original_ts)){
            uint64_t status = 0;
            if(txn_tables.get_status(original_ts,status))[[likely]]{
                if(status == IN_PROGRESS){
                    //do nothing
                }else{
                    if(status!=ABORT){
                        current_block->update_previous_delta_invalidate_ts(current_delta->toID, current_delta->previous_version_offset, status);
                        if(current_delta->lazy_update(original_ts,status)){
                            //record lazy update
                            record_lazy_update_record(&lazy_update_records, original_ts);
                        }

                    }
                    //if status == abort, must already be eager aborted, eager abort is either done by that aborted txn or the other consolidation txn
#if EDGE_DELTA_TEST
                    if(current_delta->creation_ts.load(std::memory_order_acquire)!=status){
                        throw LazyUpdateException();
                    }
#endif
                }
            }
        }
        original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
        //now check the status
        if(is_txn_id(original_ts)){
            auto in_progress_txn_deltas_emplace_result = in_progress_delta_per_txn.try_emplace(original_ts, std::vector<uint32_t>());
            in_progress_txn_deltas_emplace_result.first->second.emplace_back(original_delta_offset);
            //data_size+=current_delta->data_length+ENTRY_DELTA_SIZE;
            data_size+=ENTRY_DELTA_SIZE;
            if(current_delta->data_length>16){
                data_size+=current_delta->data_length;
            }
            to_check_delta_chains.emplace(calculate_owner_delta_chain_id(current_delta->toID, current_delta_chain_num));
        }else if(original_ts!=ABORT){
            //skip committed delete deltas
            if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                vertex_t toID = current_delta->toID;
                auto latest_version_emplace_result = edge_latest_versions_records.emplace(toID);
                //if indeed latest version
                if(latest_version_emplace_result.second){
                    edge_latest_version_offsets.emplace_back(original_delta_offset);
                    largest_creation_ts = (largest_creation_ts>=original_ts)? largest_creation_ts:original_ts;
                    data_size+=ENTRY_DELTA_SIZE;
                    if(current_delta->data_length>16){
                        data_size+= current_delta->data_length;
                    }
                }else{
                    auto  current_invalidaation_ts = current_delta->invalidate_ts.load(std::memory_order_acquire);
#if CONSOLIDATION_TEST
                    if(!current_invalidaation_ts){
                        throw LazyUpdateException();
                    }
#endif
                    if(!is_txn_id(current_invalidaation_ts)){
                        largest_invalidation_ts = (largest_invalidation_ts>=current_invalidaation_ts)? largest_invalidation_ts:current_invalidaation_ts;
                    }

                }
            }else{
                //still need to count delete delta as latest delta
                edge_latest_versions_records.emplace(current_delta->toID);
            }
        }else{
            //do nothing, can fully skip aborted deltas
        }
        original_delta_offset-=ENTRY_DELTA_SIZE;
        current_delta++;
    }
    //handle edge case that the initial block was too small
    data_size = (data_size==0)? ENTRY_DELTA_SIZE:data_size;
    data_size+=overflow_data_size+overflow_delta_size;
    //analyze scan finished, now apply heuristics
    //use the block creation time vs. latest committed write to estimate lifespan
    //uint64_t lifespan = largest_creation_ts - current_block->get_consolidation_time(); /*current_label_entry->consolidation_time;*/ //approximate lifespan of the block
    //todo:; apply different heuristics
    /*size_t new_block_size = calculate_nw_block_size_from_lifespan(data_size,lifespan,20);
    auto new_order = size_to_order(new_block_size);*/
    auto new_order = calculate_new_fit_order(data_size+sizeof(EdgeDeltaBlockHeader));
    /* if(static_cast<uint32_t>(current_block->get_order())>20){
       */
    auto new_block_ptr = block_manager.alloc(new_order);
    auto new_block = block_manager.convert<EdgeDeltaBlockHeader>(new_block_ptr);
    new_block->fill_metadata(current_block->get_owner_id(),largest_invalidation_ts, read_timestamp, current_label_entry->block_ptr,new_order, &txn_tables,current_label_entry->delta_chain_index);
    int32_t new_block_delta_chain_num = new_block->get_delta_chain_num();
    std::vector<AtomicDeltaOffset> new_delta_chains_index(new_block_delta_chain_num);
    //start installing latest version
    int64_t signed_edge_last_version_size = static_cast<int64_t>(edge_latest_version_offsets.size());
    for(int64_t i = (signed_edge_last_version_size-1); i>=0; i--){
        current_delta = current_block->get_edge_delta(edge_latest_version_offsets.at(i));
        if(is_txn_id(current_delta->creation_ts.load(std::memory_order_acquire)))[[unlikely]]{
            throw ConsolidationException();//latest versions should all be committed deltas
        }
        //find which delta chain the latest version delta belongs to
        delta_chain_id_t target_delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
        char* data; //= current_block->get_edge_data(current_delta->data_offset);
        if(current_delta->data_length<=16){
            data = current_delta->data;
        }else{
            data = current_block->get_edge_data(current_delta->data_offset);
        }
        auto& new_delta_chains_index_entry = new_delta_chains_index.at(target_delta_chain_id);
        uint32_t new_block_delta_offset = new_delta_chains_index_entry.get_offset();//if cannot be locked
        //install latest version delta
        auto consolidation_append_result = new_block->append_edge_delta(current_delta->toID,current_delta->creation_ts.load(),current_delta->delta_type,data,current_delta->data_length,new_block_delta_offset);
        if(consolidation_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!consolidation_append_result.second)[[unlikely]]{
            throw ConsolidationException();
        }
        new_delta_chains_index_entry.update_offset(consolidation_append_result.second);
    }
    //until this step: it is the same as normal Consolidation
    //we installed all committed latest versions and form delta chains
    //now we start the exclusive installation phase
    BlockStateVersionProtectionScheme::install_exclusive_state(EdgeDeltaBlockState::INSTALLATION,thread_id,block_id,current_label_entry,block_access_ts_table);
    //wait for all validating transactions to finish
    while(true){
        for(auto it = to_check_delta_chains.begin(); it!= to_check_delta_chains.end();){
            //get the delta chain head of this delta chain in the original block, check if a validation is happening
            current_delta = current_block->get_edge_delta(current_label_entry->delta_chain_index->at(*it).get_raw_offset());
            uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
            if(is_txn_id(original_ts)){
                uint64_t status =0;
                if(txn_tables.get_status(original_ts,status)){
                    //this transaction still validating, so we will come back later
                    if(status == IN_PROGRESS){
                        it++;
                        continue;
                    }else if(status ==ABORT){ //validating txn aborted, so consolidating thread will help with lazy update, its deltas in the current block is all or nothing (eager aborted)
                        uint32_t to_abort_offset = current_delta->previous_offset;
                        if(!current_delta->lazy_update(original_ts,status)){
                            throw ConsolidationException();//we should never fail lazy update during an exclusive state
                        }
                        auto abort_lazy_update_emplace_result = lazy_update_records.try_emplace(original_ts,1);
                        if(!abort_lazy_update_emplace_result.second){
                            abort_lazy_update_emplace_result.first->second++;
                        }
                        while(to_abort_offset!=0){
                            current_delta = current_block->get_edge_delta(to_abort_offset);
                            if(current_delta->creation_ts.load(std::memory_order_acquire)!=original_ts){
                                break;
                            }else if(current_delta->creation_ts.load(std::memory_order_acquire)==original_ts){
                                if(current_delta->lazy_update(original_ts,status)){
                                    abort_lazy_update_emplace_result.first->second++;
                                }else{
                                    throw ConsolidationException();
                                }
                            }
#if CONSOLIDATION_TEST
                            else if(current_delta->creation_ts.load(std::memory_order_acquire)==ABORT){
                                throw EagerAbortException();//aborted delta should not exit in the delta chain (except my own deltas that I'm aborting )
                            }
#endif
                            to_abort_offset = current_delta->previous_offset;
                        }
                        it = to_check_delta_chains.erase(it);//this delta chain is cleaned
                    }else{//original_ts txn committed after passing validation phase
                        uint32_t to_commit_offset = current_delta->previous_offset;
                        if(!current_delta->lazy_update(original_ts, status)){
                            throw ConsolidationException();
                        }
                        auto commit_lazy_update_emplace_result = lazy_update_records.try_emplace(original_ts,1);
                        if(!commit_lazy_update_emplace_result.second){
                            commit_lazy_update_emplace_result.first->second++;
                        }
                        while(to_commit_offset!=0){
                            current_delta= current_block->get_edge_delta(to_commit_offset);
                            if(current_delta->creation_ts.load(std::memory_order_acquire)!=original_ts&&current_delta->creation_ts.load(std::memory_order_acquire)!=status){//someone can already lazy update parts of txn's deltas due to concurrent readers
#if EDGE_DELTA_TEST
                                if(current_delta->creation_ts.load(std::memory_order_acquire)==ABORT){
                                    throw LazyUpdateException();
                                }
#endif
                                break;
                            }else if(current_delta->creation_ts.load(std::memory_order_acquire)==original_ts){
                                if(!current_delta->lazy_update(original_ts,status)){
                                    throw ConsolidationException();
                                }
                                commit_lazy_update_emplace_result.first->second++;
                            }
                            to_commit_offset = current_delta->previous_offset;
                        }
                        it = to_check_delta_chains.erase(it);
                    }
                }else{
                    throw ConsolidationException();//Installation phase is supposed to be exclusive.
                }
            }else{
                //this delta chain is not under validating or someone already lazy updated for it
                it = to_check_delta_chains.erase(it);
            }
        }
        if(to_check_delta_chains.empty()){
            break;
        }
    }
    //now check all in progress deltas recorded earlier
    //need 2 for loops because committed deltas should be before in-progress deltas
    for(auto it = in_progress_delta_per_txn.begin();it!=in_progress_delta_per_txn.end();){
        auto& all_delta_offsets_of_txn = it->second;
        current_delta = current_block->get_edge_delta(all_delta_offsets_of_txn.at(0));
        //skip them, will be installed at the end.
        if(is_txn_id(current_delta->creation_ts.load(std::memory_order_acquire))){
            it++;
        }else if(current_delta->creation_ts.load(std::memory_order_acquire)==ABORT){
            it = in_progress_delta_per_txn.erase(it);
        }else{//committed deltas
            timestamp_t commit_ts = current_delta->creation_ts.load(std::memory_order_acquire);
            int64_t txn_own_deltas_size = static_cast<int64_t>(all_delta_offsets_of_txn.size());
            for(int64_t i = txn_own_deltas_size-1; i>=0; i--){
                uint64_t txn_id = it->first;
                current_delta= current_block->get_edge_delta(all_delta_offsets_of_txn.at(i));
                if(current_delta->lazy_update(txn_id,commit_ts)){
                    //todo:: delete this
                    record_lazy_update_record(&lazy_update_records,txn_id);//todo: will this really happen?
                }
                //only install non-delete deltas
                if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                    //const char* data = current_block->get_edge_data(current_delta->data_offset);
                    char* data;
                    if(current_delta->data_length<=16){
                        data = current_delta->data;
                    }else{
                        data = current_block->get_edge_data(current_delta->data_offset);
                    }
                    delta_chain_id_t new_delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
                    auto& new_delta_chains_index_entry = new_delta_chains_index.at(new_delta_chain_id);
                    uint32_t previous_version_offset=0;
                    if(current_delta->delta_type==EdgeDeltaType::UPDATE_DELTA){
                        //get potential previous version and install invalidation ts
                        if(new_delta_chains_index_entry.get_offset())
                        {
#if USING_SIMPLE_PREVIOUS_VERSION
                            previous_version_offset = new_block->fetch_previous_version_offset_simple(current_delta->toID,new_delta_chains_index_entry.get_offset(),current_delta->creation_ts.load());
#else
                            previous_version_offset = new_block->fetch_previous_version_offset(current_delta->toID,new_delta_chains_index_entry.get_offset(),current_delta->creation_ts.load(),lazy_update_records);
#endif
                        }
#if EDGE_DELTA_TEST
                        if(!previous_version_offset){
                            throw std::runtime_error("error, under checked version, an update delta must have a previous version");
                        }
#endif
                    }
                    //can get raw offset because no lock yet
                    // auto commit_delta_append_result = new_block->append_edge_delta(current_delta->toID,commit_ts,current_delta->delta_type,data,current_delta->data_length,new_delta_chains_index_entry.get_offset());
                    auto commit_delta_append_result = new_block->checked_append_edge_delta(current_delta->toID,commit_ts,current_delta->delta_type,data,current_delta->data_length,new_delta_chains_index_entry.get_offset(),previous_version_offset);
                    //can be a newer version:
                    //new_block->update_previous_delta_invalidate_ts(current_delta->toID,new_delta_chains_index_entry.get_offset(),commit_ts);
                    if(commit_delta_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!commit_delta_append_result.second){
                        throw ConsolidationException();
                    }
                    new_delta_chains_index_entry.update_offset(commit_delta_append_result.second);
                }else{//need to install committed delete deltas in the new block
                    delta_chain_id_t new_delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
                    auto& new_delta_chains_index_entry = new_delta_chains_index.at(new_delta_chain_id);
                    //get potential previous version and install invalidation ts
                    //a committed delete delta must have a previous version of the edge, so update the invalidation ts of previous version and get its offset

                    uint32_t previous_version_offset = 0;
                    if(new_delta_chains_index_entry.get_offset())
#if USING_SIMPLE_PREVIOUS_VERSION
                        previous_version_offset= new_block->fetch_previous_version_offset_simple(current_delta->toID,new_delta_chains_index_entry.get_offset(),current_delta->creation_ts.load());
#else
                        previous_version_offset= new_block->fetch_previous_version_offset(current_delta->toID,new_delta_chains_index_entry.get_offset(),current_delta->creation_ts.load(),lazy_update_records);
#endif
#if EDGE_DELTA_TEST
                    if(!previous_version_offset){
                        throw std::runtime_error("error, under checked version, a delete delta must have a previous version");
                    }
#endif
                    auto commit_delta_append_result = new_block->checked_append_edge_delta(current_delta->toID,commit_ts,current_delta->delta_type,nullptr,current_delta->data_length,new_delta_chains_index_entry.get_offset(),previous_version_offset);
                    //can be a newer version:
                    //new_block->update_previous_delta_invalidate_ts(current_delta->toID,new_delta_chains_index_entry.get_offset(),commit_ts);
                    if(commit_delta_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!commit_delta_append_result.second){
                        throw ConsolidationException();
                    }
                    new_delta_chains_index_entry.update_offset(commit_delta_append_result.second);
                }
            }
            it = in_progress_delta_per_txn.erase(it);
        }
    }
    //finally install those still in-progress deltas per txn
    //they should contain previous version offset as well, but should not
    //update previous version invalidate ts is safe, no real conflict would happen with our locking protocol
    for(auto it = in_progress_delta_per_txn.begin(); it!=in_progress_delta_per_txn.end();it++){
        uint64_t txn_id = it->first;
#if CONSOLIDATION_TEST
        uint64_t status;
        if(txn_tables.get_status(txn_id, status)){
            if(status!=IN_PROGRESS){
                throw ConsolidationException();
            }
        }else{
            throw ConsolidationException();
        }
#endif
        auto& txn_in_progress_deltas = it->second;
        std::vector<uint32_t>local_delta_chains_index_cache(new_block_delta_chain_num);
        for(int32_t j=0; j<new_block_delta_chain_num;j++){
            local_delta_chains_index_cache[j]=new_delta_chains_index[j].get_offset();
        }
        int64_t txn_delta_num = static_cast<int64_t>(txn_in_progress_deltas.size());
        for(int64_t i =txn_delta_num-1; i>=0; i--){
            current_delta = current_block->get_edge_delta(txn_in_progress_deltas.at(i));
#if CONSOLIDATION_TEST
            if(current_delta->creation_ts.load(std::memory_order_acquire)!=txn_id){
                throw ConsolidationException();
            }
#endif
            char* data;
            if(current_delta->data_length<=16){
                data = current_delta->data;
            }else{
                data = current_block->get_edge_data(current_delta->data_offset);
            }
            delta_chain_id_t delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
            uint32_t previous_version_offset = 0;
            if(current_delta->delta_type!=EdgeDeltaType::INSERT_DELTA){
                if(local_delta_chains_index_cache[delta_chain_id])
                {
#if USING_SIMPLE_PREVIOUS_VERSION
                    previous_version_offset = new_block->fetch_previous_version_offset_simple(current_delta->toID,local_delta_chains_index_cache[delta_chain_id],current_delta->creation_ts.load());
#else
                    previous_version_offset = new_block->fetch_previous_version_offset(current_delta->toID,local_delta_chains_index_cache[delta_chain_id],current_delta->creation_ts.load(),lazy_update_records);
#endif
                }
#if EDGE_DELTA_TEST
                if(!previous_version_offset){
                    throw std::runtime_error("error, under checked version, a delete or update delta must have a previous version");
                }
#endif
            }
            auto in_progress_delta_append_result = new_block->checked_append_edge_delta(current_delta->toID, txn_id, current_delta->delta_type, data, current_delta->data_length, local_delta_chains_index_cache[delta_chain_id],previous_version_offset);
#if CONSOLIDATION_TEST
            if(in_progress_delta_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!in_progress_delta_append_result.second){
                throw ConsolidationException();
            }
#endif
            local_delta_chains_index_cache[delta_chain_id]= in_progress_delta_append_result.second;
        }
    }
    //now consolidation is over
    per_thread_garbage_queue.register_entry(current_label_entry->block_ptr,current_block->get_order(),largest_invalidation_ts);
#if !USING_COMMIT_WAIT_WORK
    if(per_thread_garbage_queue.need_collection()){
        auto safe_ts = block_access_ts_table.calculate_safe_ts();
        per_thread_garbage_queue.free_block(safe_ts);
    }
#endif
    *current_label_entry->delta_chain_index = std::move(new_delta_chains_index);//todo::check its correctness
    current_label_entry->block_ptr = new_block_ptr;
/*    if(new_block->already_overflow()){
        throw ConsolidationException();
    }*/
    current_label_entry->block_version_number.fetch_add(1,std::memory_order_acq_rel);//increase version by 1 /*consolidation_time.store(commit_manager.get_current_read_ts());*/
    BlockStateVersionProtectionScheme::install_shared_state(EdgeDeltaBlockState::NORMAL,current_label_entry);
    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
}

/*
 * scan the previous block to locate an edge
 */
std::string_view
RWTransaction::scan_previous_block_find_edge(GTX::EdgeDeltaBlockHeader *previous_block, GTX::vertex_t vid) {
    uint64_t combined_offset = previous_block->get_current_offset();
    current_delta_offset = EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(combined_offset);
    BaseEdgeDelta* current_delta = previous_block->get_edge_delta(current_delta_offset);
    while(current_delta_offset>0){
        if(current_delta->toID==vid){
            uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
            //abort and txn id are both quite large
            if(original_ts<=read_timestamp){
                //return std::string_view (previous_block->get_edge_data(current_delta->data_offset),current_delta->data_length);
                if(current_delta->data_length<=16){
                    return std::string_view (current_delta->get_data(),current_delta->data_length);
                }else{
                    return std::string_view (previous_block->get_edge_data(current_delta->data_offset),current_delta->data_length);
                }
            }
        }
        current_delta ++;
        current_delta_offset-=ENTRY_DELTA_SIZE;
    }
    return std::string_view();
}

std::pair<Txn_Operation_Response, std::string_view>
RWTransaction::get_edge(GTX::vertex_t src, GTX::vertex_t dst, GTX::label_t label) {
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry)[[unlikely]]{
        return std::pair<Txn_Operation_Response, std::string_view>(Txn_Operation_Response::FAIL,std::string_view());
    }
    auto block_id = generate_block_id(src,label);
    if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry,block_access_ts_table))[[likely]]{
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        auto cached_delta_offsets =per_block_cached_delta_chain_offsets.find(block_id);
        uint32_t offset = 0;
        //if we have cache
        if(cached_delta_offsets!=per_block_cached_delta_chain_offsets.end()){
            if(cached_delta_offsets->second.is_outdated(target_label_entry->block_version_number.load(std::memory_order_acquire)))[[unlikely]]{
                auto reclaim_result = reclaim_delta_chain_offsets(cached_delta_offsets->second,current_block,target_label_entry);
                if(reclaim_result==ReclaimDeltaChainResult::FAIL){
                    per_block_cached_delta_chain_offsets.erase(cached_delta_offsets->first);//already eager aborted, so the entry can be aborted.
                    return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::FAIL,std::string_view());
                }else if(reclaim_result==ReclaimDeltaChainResult::RETRY){
                    return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::READER_WAIT,std::string_view());
                }//otherwise succeeds and continue forward
            }
            //if no version change or reclaim all our deltas
            offset = cached_delta_offsets->second.is_edge_already_locked(dst);
        }
        //if current txn has writes in the edge delta block
        if(offset)[[unlikely]]{
            BaseEdgeDelta* target_delta;
            if(offset<=8*ENTRY_DELTA_SIZE){

                target_delta = current_block->get_visible_target_using_scan(offset,dst,read_timestamp,lazy_update_records,local_txn_id);
            }else{
                target_delta = current_block->get_visible_target_delta_using_delta_chain(offset,dst,read_timestamp,lazy_update_records,local_txn_id);
            }
#if EDGE_DELTA_TEST
            if(!target_delta||target_delta->toID!=dst){
                throw TransactionReadException();
            }
#endif

            char* data;
            if(target_delta->data_length<=16){
                data = target_delta->data;
            }else{
                data = current_block->get_edge_data(target_delta->data_offset);
            }
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            record_edge_read(generate_block_id(src,label),dst);
            return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::SUCCESS, std::string_view(data,target_delta->data_length));
        }else{
            //determine which block to read
            if(read_timestamp>=current_block->get_creation_time())[[likely]]{
                auto& delta_chains_index_entry = target_label_entry->delta_chain_index->at(current_block->get_delta_chain_id(dst));
                offset = delta_chains_index_entry.get_raw_offset();//may be locked
                //check delta chain head and early abort
                if(offset)[[likely]]{
                    auto current_delta = current_block->get_edge_delta(offset);
                    auto original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                    if(is_txn_id(original_ts))[[unlikely]]{
                        uint64_t status = 0;
                        if(txn_tables.get_status(original_ts,status))[[likely]]{
                            if(status == IN_PROGRESS){
                                //do nothing
                            }else{
                                if(status!=ABORT){
                                    current_block->update_previous_delta_invalidate_ts(current_delta->toID, current_delta->previous_version_offset, status);
                                    if(current_delta->lazy_update(original_ts,status)){
                                        //record lazy update
                                        record_lazy_update_record(&lazy_update_records, original_ts);
                                    }

                                }
                                //if status == abort, must already be eager aborted, eager abort is either done by that aborted txn or the other consolidation txn
#if EDGE_DELTA_TEST
                                if(current_delta->creation_ts.load(std::memory_order_acquire)!=status){
                        throw LazyUpdateException();
                    }
#endif
                            }
                        }
                        original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                    }
                    if(original_ts>read_timestamp){
                        BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                        return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::FAIL,std::string_view());
                    }
                }
                BaseEdgeDelta* target_delta= nullptr;

                if(offset)[[likely]]{
                    if(offset<=8*ENTRY_DELTA_SIZE){

                        target_delta = current_block->get_visible_target_using_scan(offset,dst,read_timestamp,lazy_update_records,local_txn_id);
                    }else{
                        target_delta = current_block->get_visible_target_delta_using_delta_chain(offset,dst,read_timestamp,lazy_update_records,local_txn_id);
                    }
                }
                if(target_delta)[[likely]]{
                    char* data;
                    if(target_delta->data_length<=16){
                        data = target_delta->data;
                    }else{
                        data = current_block->get_edge_data(target_delta->data_offset);
                    }
#if EDGE_DELTA_TEST
                    if(target_delta->toID!=dst){
                        throw TransactionReadException();
                    }
#endif
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    record_edge_read(generate_block_id(src,label),dst);
                    return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::SUCCESS, std::string_view(data,target_delta->data_length));
                }else{
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    record_edge_read(generate_block_id(src,label),dst);
                    return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::SUCCESS,std::string());
                }
            }else{
                //serializable version prohibits reading previous block version
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::FAIL,std::string_view());
                /*
                if(current_block->get_previous_ptr())[[likely]]{
                    EdgeDeltaBlockHeader* previous_block = block_manager.convert<EdgeDeltaBlockHeader>(current_block->get_previous_ptr());
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);//reading previous block needs no protection, it is protected by read epoch
                    //if current block is still too old
                    while(read_timestamp<previous_block->get_creation_time()){
                        //if can go even further, then go there
                        if(previous_block->get_previous_ptr()){
                            previous_block = block_manager.convert<EdgeDeltaBlockHeader>(previous_block->get_previous_ptr());
                        }else{//if no more previous block exist, just return empty view
                            return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::SUCCESS,
                                                                                      std::string_view());
                        }
                    }
                    return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::SUCCESS,
                                                                              scan_previous_block_find_edge(previous_block,dst));
                }else{
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);//reading previous block needs no protection, it is protected by read epoch
                    return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::SUCCESS,
                                                                              std::string_view());
                }*/
            }
        }
    }else{
#if ENABLE_VERTEX_DELETION
        auto current_state = target_label_entry->state.load(std::memory_order_acquire);
        if(current_state==EdgeDeltaBlockState::DELETION)[[unlikely]]{
            return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::FAIL,std::string_view());
        }
#endif
        return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::READER_WAIT,std::string_view());
    }
}
/*
 * should not use this, it overcomplicates read-write transaction scanning adjacency list
 */
std::pair<Txn_Operation_Response, EdgeDeltaIterator>
RWTransaction::get_edges(GTX::vertex_t src, GTX::label_t label) {
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry){
        return std::pair<Txn_Operation_Response, EdgeDeltaIterator>(Txn_Operation_Response::FAIL,EdgeDeltaIterator());
    }
    auto block_id = generate_block_id(src, label);
    if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry,block_access_ts_table)){
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        auto cached_delta_offsets =per_block_cached_delta_chain_offsets.find(block_id);
        bool has_deltas = false;
        if(cached_delta_offsets!=per_block_cached_delta_chain_offsets.end()){
            //eagerly check if our cache is outdated
            if(cached_delta_offsets->second.is_outdated(target_label_entry->block_version_number.load(std::memory_order_acquire))){
                auto reclaim_result = reclaim_delta_chain_offsets(cached_delta_offsets->second,current_block,target_label_entry);
                //block protection released by reclaim_delta_chain_offsets()
                if(reclaim_result==ReclaimDeltaChainResult::FAIL){
                    per_block_cached_delta_chain_offsets.erase(cached_delta_offsets->first);//already eager aborted, so the entry can be aborted.
                    return std::pair<Txn_Operation_Response,EdgeDeltaIterator>(Txn_Operation_Response::FAIL,EdgeDeltaIterator());
                    //block protection released by reclaim_delta_chain_offsets()
                }else if(reclaim_result==ReclaimDeltaChainResult::RETRY){
                    return std::pair<Txn_Operation_Response,EdgeDeltaIterator>(Txn_Operation_Response::READER_WAIT,EdgeDeltaIterator());
                }//otherwise succeeds and continue forward
            }
            has_deltas=true;
        }
        uint64_t current_combined_offset = current_block->get_current_offset();
        if(current_block->is_overflow_offset(current_combined_offset)){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return std::pair<Txn_Operation_Response,EdgeDeltaIterator>(Txn_Operation_Response::READER_WAIT,EdgeDeltaIterator());
        }
        //block protection released by iterator destructor
        scanned_adjacency_list.emplace(generate_block_id(src,label));
        return std::pair<Txn_Operation_Response,EdgeDeltaIterator>(Txn_Operation_Response::SUCCESS,EdgeDeltaIterator(current_block,read_timestamp,local_txn_id,has_deltas,EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset),
                                                                                                                     graph,&lazy_update_records,&block_access_ts_table));
    }else{
#if ENABLE_VERTEX_DELETION
        auto current_state = target_label_entry->state.load(std::memory_order_acquire);
        if(current_state==EdgeDeltaBlockState::DELETION)[[unlikely]]{
            return std::pair<Txn_Operation_Response,EdgeDeltaIterator>(Txn_Operation_Response::FAIL,EdgeDeltaIterator());
        }
#endif
        return std::pair<Txn_Operation_Response, EdgeDeltaIterator>(Txn_Operation_Response::READER_WAIT,EdgeDeltaIterator());
    }
}

/*
 * scan the neighborhood of a vertex of a label
 */
std::pair<Txn_Operation_Response, SimpleEdgeDeltaIterator>
RWTransaction::simple_get_edges(GTX::vertex_t src, GTX::label_t label) {
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry){
        return std::pair<Txn_Operation_Response, SimpleEdgeDeltaIterator>(Txn_Operation_Response::FAIL,SimpleEdgeDeltaIterator());
    }
    auto block_id = generate_block_id(src, label);
    if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry,block_access_ts_table)){
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        auto cached_delta_offsets =per_block_cached_delta_chain_offsets.find(block_id);
        bool has_deltas = false;
        if(cached_delta_offsets!=per_block_cached_delta_chain_offsets.end()){
            //eagerly check if our cache is outdated
            if(cached_delta_offsets->second.is_outdated(target_label_entry->block_version_number.load(std::memory_order_acquire))){
                auto reclaim_result = reclaim_delta_chain_offsets(cached_delta_offsets->second,current_block,target_label_entry);
                //block protection released by reclaim_delta_chain_offsets()
                if(reclaim_result==ReclaimDeltaChainResult::FAIL){
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    per_block_cached_delta_chain_offsets.erase(cached_delta_offsets->first);//already eager aborted, so the entry can be aborted.
                    return std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator>(Txn_Operation_Response::FAIL,SimpleEdgeDeltaIterator());
                    //block protection released by reclaim_delta_chain_offsets()
                }else if(reclaim_result==ReclaimDeltaChainResult::RETRY){
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    return std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator>(Txn_Operation_Response::READER_WAIT,SimpleEdgeDeltaIterator());
                }//otherwise succeeds and continue forward
            }
            has_deltas=true;
        }
        uint64_t current_combined_offset = current_block->get_current_offset();
        if(current_block->is_overflow_offset(current_combined_offset)){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator>(Txn_Operation_Response::READER_WAIT,SimpleEdgeDeltaIterator());
        }
        //check versioning
        if(current_block->get_creation_time()> read_timestamp&&has_deltas){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator>(Txn_Operation_Response::FAIL,SimpleEdgeDeltaIterator());//make life easier
        }
        //block protection released by iterator destructor
        scanned_adjacency_list.emplace(generate_block_id(src,label));
        return std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator>(Txn_Operation_Response::SUCCESS,SimpleEdgeDeltaIterator(current_block,read_timestamp,local_txn_id,EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset),
                                                                                                                                 graph,&lazy_update_records,&block_access_ts_table));
    }else{
#if ENABLE_VERTEX_DELETION
        auto current_state = target_label_entry->state.load(std::memory_order_acquire);
        if(current_state==EdgeDeltaBlockState::DELETION)[[unlikely]]{
            return std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator>(Txn_Operation_Response::FAIL,SimpleEdgeDeltaIterator());
        }
#endif
        return std::pair<Txn_Operation_Response, SimpleEdgeDeltaIterator>(Txn_Operation_Response::READER_WAIT,SimpleEdgeDeltaIterator());
    }
}

/*
 * if invoked during normal execution, the txn should eager abort all its deltas in this function
 * if invoked during validation, the txn will try to eager abort the blocks the txn already validated
 * (some may be impossible due to Installation state) and remove those entries from the map. The remaining ones must be eager aborted
 */
void RWTransaction::eager_abort() {
    while(true){
        for(auto touched_block_it = per_block_cached_delta_chain_offsets.begin(); touched_block_it!= per_block_cached_delta_chain_offsets.end();){
            //if the txn did not update any real deltas, it can just skip this entry?
            if(touched_block_it->second.already_modified_edges.empty()){
                touched_block_it = per_block_cached_delta_chain_offsets.erase(touched_block_it);
                continue;
            }
            auto current_label_entry = get_label_entry(touched_block_it->first);
            auto block_access_result = BlockStateVersionProtectionScheme::committer_aborter_access_block(thread_id,touched_block_it->first,current_label_entry,block_access_ts_table);
            //if has protection now
            if(block_access_result==EdgeDeltaBlockState::NORMAL||block_access_result==EdgeDeltaBlockState::CONSOLIDATION)[[likely]]{
                auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(current_label_entry->block_ptr);
                uint64_t current_combined_offset = current_block->get_current_offset();
                if(current_block->is_overflow_offset(current_combined_offset)){
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    continue;
                }
                if(!touched_block_it->second.is_outdated(current_label_entry->block_version_number.load(std::memory_order_acquire))){
                    touched_block_it->second.release_protections(current_block);//if same version, we need to release locks
                }
                op_count -=touched_block_it->second.eager_abort(current_block,current_label_entry,local_txn_id,current_combined_offset);
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                touched_block_it = per_block_cached_delta_chain_offsets.erase(touched_block_it);
            }
#if ENABLE_VERTEX_DELETION
            else if(block_access_result==EdgeDeltaBlockState::DELETION){
                //just let the deleter to do the job?
                touched_block_it = per_block_cached_delta_chain_offsets.erase(touched_block_it);
            }
#endif
            else{
                touched_block_it++;
            }
        }
        if(!updated_vertices.empty()){
            //eager abort of vertex deltas should always immediately succeeds
            for(auto touched_vertex_it = updated_vertices.begin();touched_vertex_it!=updated_vertices.end();touched_vertex_it++){
                auto& vertex_index_entry = graph.get_vertex_index_entry(*touched_vertex_it);
                uint64_t current_vertex_delta_ptr = vertex_index_entry.vertex_delta_chain_head_ptr.load(std::memory_order_acquire);
                auto current_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta_ptr);
#if TXN_TEST
                if(current_vertex_delta->get_creation_ts()!=local_txn_id){
                    throw EagerAbortException();
                }
#endif
                vertex_index_entry.vertex_delta_chain_head_ptr.store(current_vertex_delta->get_previous_ptr());
                current_vertex_delta->eager_abort();
                per_thread_garbage_queue.register_entry(current_vertex_delta_ptr, current_vertex_delta->get_order() , commit_manager.get_current_read_ts());
#if !USING_COMMIT_WAIT_WORK
                if(per_thread_garbage_queue.need_collection()){
                    auto safe_ts = block_access_ts_table.calculate_safe_ts();
                    per_thread_garbage_queue.free_block(safe_ts);
                }
#endif
                op_count--;
            }
            updated_vertices.clear();
        }
        if(!created_vertices.empty()){
            for(auto created_vertex_it = created_vertices.begin();created_vertex_it!= created_vertices.end();created_vertex_it++){
                auto& vertex_index_entry = graph.get_vertex_index_entry(*created_vertex_it);
                vertex_index_entry.valid.store(false,std::memory_order_release);
                thread_local_recycled_vertices.push(*created_vertex_it);//recycle this vid
            }
            created_vertices.clear();
        }
        if(per_block_cached_delta_chain_offsets.empty()){
#if TXN_TEST
            if(op_count<0){
                throw EagerAbortException();
            }else if(op_count>0){
               // std::cout<<"need lazy abort"<<std::endl;
            }
#endif
            return;
        }
    }
}

/*
 * should not be called, too complicated
 */
bool RWTransaction::validation() {
    bool to_abort = false;
    std::map<uint64_t, std::vector<validation_to_revise_entry>>already_validated_offsets_per_block;
    for(auto touched_block_it = per_block_cached_delta_chain_offsets.begin();touched_block_it!= per_block_cached_delta_chain_offsets.end();){
        auto validated_offset_cache = already_validated_offsets_per_block.try_emplace(touched_block_it->first,std::vector<validation_to_revise_entry>());
        auto current_label_entry = get_label_entry(touched_block_it->first);
        auto validation_access_result = BlockStateVersionProtectionScheme::committer_aborter_access_block(thread_id,touched_block_it->first,current_label_entry,block_access_ts_table);
        if(validation_access_result == EdgeDeltaBlockState::NORMAL || validation_access_result== EdgeDeltaBlockState::CONSOLIDATION){
            if(touched_block_it->second.is_outdated(current_label_entry->block_version_number)){
                auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(current_label_entry->block_ptr);
                uint64_t current_combined_offset = current_block->get_current_offset();
                if(current_block->is_overflow_offset(current_combined_offset)){
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    continue;//offset should be quickly restored, let's exit and retry soon
                }
                bool reclaim_delta_chains_locks_result = touched_block_it->second.reclaim_delta_chain_lock(current_block,current_label_entry,local_txn_id,read_timestamp,current_combined_offset);
                if(!reclaim_delta_chains_locks_result){
                    //txn already lost all its locks in this block, eager abort this block with no revise index or lock release
                    op_count-=touched_block_it->second.eager_abort(current_block,current_label_entry,local_txn_id,current_combined_offset);
                    BlockStateVersionProtectionScheme::release_protection(thread_id, block_access_ts_table);
                    to_abort=true;
                    per_block_cached_delta_chain_offsets.erase(touched_block_it);
                    break;//exist validation, start aborting
                }
            }
            //either finish reclaiming or no version change took place, let's commit to delta chains index
            for(auto cached_offset_it = touched_block_it->second.already_updated_delta_chain_head_offsets.begin();cached_offset_it!=touched_block_it->second.already_updated_delta_chain_head_offsets.end();cached_offset_it++){
                validated_offset_cache.first->second.emplace_back(cached_offset_it->first,current_label_entry->delta_chain_index->at(cached_offset_it->first).get_raw_offset());//cache the to-restore value without the lock, if we need to restore, release the lock together
                current_label_entry->delta_chain_index->at(cached_offset_it->first).update_offset(cached_offset_it->second|LOCK_MASK);//todo: check this, should update to a locked version! But our local cache should always store unlocked version
                //todo:: in the simplified version, update directly to unlocked offset.
            }
            BlockStateVersionProtectionScheme::release_protection(thread_id, block_access_ts_table);
            touched_block_it++;
        }
        //otherwise txn has to wait here, avoid deadlock
    }
    if(to_abort){
        for(auto reverse_it = already_validated_offsets_per_block.begin(); reverse_it!= already_validated_offsets_per_block.end();){
            if(reverse_it->second.empty()){
                reverse_it++;
                continue;
            }
            auto current_label_entry = get_label_entry(reverse_it->first);
            auto validation_state = BlockStateVersionProtectionScheme::committer_aborter_access_block(thread_id,reverse_it->first,current_label_entry,block_access_ts_table);
            if(validation_state==EdgeDeltaBlockState::NORMAL||validation_state==EdgeDeltaBlockState::CONSOLIDATION){
                auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(current_label_entry->block_ptr);
                auto& to_reverse_offsets = reverse_it->second;
                for(size_t i=0; i<to_reverse_offsets.size();i++){
                    auto& entry = to_reverse_offsets.at(i);
                    //restore offset and release lock
                    current_label_entry->delta_chain_index->at(entry.delta_chain_id).update_offset(entry.original_offset);//todo: original offset (in the index) should always be lock at the current design, so we release the lock and restore offset together
                }
                auto validated_offsets_cache = per_block_cached_delta_chain_offsets.find(reverse_it->first);
                if(validated_offsets_cache->second.is_outdated(current_label_entry->block_version_number.load(std::memory_order_acquire))){
                    throw DeltaChainOffsetException();
                }
                //lock released in updating offsets already
                op_count -= validated_offsets_cache->second.eager_abort(current_block,current_label_entry,local_txn_id,0);//must use cache
                per_block_cached_delta_chain_offsets.erase(validated_offsets_cache);
                reverse_it++;
                BlockStateVersionProtectionScheme::release_protection(thread_id, block_access_ts_table);
            }else if(validation_state== EdgeDeltaBlockState::INSTALLATION){//mutex state already entered, so we will move forward, let consolidation thread observes our state and abort our deltas
                per_block_cached_delta_chain_offsets.erase(reverse_it->first);
                reverse_it++;
                continue;
            }else if(validation_state == EdgeDeltaBlockState::OVERFLOW){
                continue;//overflow state is transient so we can just come back later to valdiate in another state
            }else{
                //error state
                throw ValidationException();
            }
        }
        return false;
    }
    return true;
}

bool RWTransaction::simple_validation() {
    if(per_block_cached_delta_chain_offsets.empty())[[unlikely]]{
        return true;
    }
    bool to_abort = false;
    std::map<uint64_t, std::vector<validation_to_revise_entry>>already_validated_offsets_per_block;
    for(auto touched_block_it = per_block_cached_delta_chain_offsets.begin();touched_block_it!= per_block_cached_delta_chain_offsets.end();){
        auto validated_offset_cache = already_validated_offsets_per_block.try_emplace(touched_block_it->first,std::vector<validation_to_revise_entry>());
        auto current_label_entry = get_label_entry(touched_block_it->first);
        auto validation_access_result = BlockStateVersionProtectionScheme::committer_aborter_access_block(thread_id,touched_block_it->first,current_label_entry,block_access_ts_table);
        //for delete version
#if ENABLE_VERTEX_DELETION
        if(validation_access_result==EdgeDeltaBlockState::DELETION){
            to_abort=true;
            break;
        }
#endif
        if(validation_access_result == EdgeDeltaBlockState::NORMAL || validation_access_result== EdgeDeltaBlockState::CONSOLIDATION){
#if TXN_TEST
            uint32_t outdate_code =0;
#endif
            if(touched_block_it->second.is_outdated(current_label_entry->block_version_number)){
#if TXN_TEST
                outdate_code =13881749;
#endif
                auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(current_label_entry->block_ptr);
                uint64_t current_combined_offset = current_block->get_current_offset();
                if(current_block->is_overflow_offset(current_combined_offset)){
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    continue;//offset should be quickly restored, let's exit and retry soon
                }
                bool reclaim_delta_chains_locks_result = touched_block_it->second.reclaim_delta_chain_lock(current_block,current_label_entry,local_txn_id,read_timestamp,current_combined_offset,&lazy_update_records);
                if(!reclaim_delta_chains_locks_result){
                    //txn already lost all its locks in this block, eager abort this block with no revise index or lock release
                    op_count-=touched_block_it->second.eager_abort(current_block,current_label_entry,local_txn_id,current_combined_offset);
                    BlockStateVersionProtectionScheme::release_protection(thread_id, block_access_ts_table);
                    to_abort=true;
                    per_block_cached_delta_chain_offsets.erase(touched_block_it);
                    //std::cout<<"validation failed"<<std::endl;
                    break;//exist validation, start aborting
                }
            }
            //either finish reclaiming or no version change took place, let's commit to delta chains index
            for(auto cached_offset_it = touched_block_it->second.already_updated_delta_chain_head_offsets.begin();cached_offset_it!=touched_block_it->second.already_updated_delta_chain_head_offsets.end();cached_offset_it++){
#if TXN_TEST
                uint32_t locked_offset = current_label_entry->delta_chain_index->at(cached_offset_it->first).get_offset();
                if(!(locked_offset&LOCK_MASK)){
                    std::cout<<"outdate code is "<<outdate_code<<std::endl;
                    std::cout<<"locked offset is "<<locked_offset<<std::endl;
                    std::cout<<"cached offset is "<<cached_offset_it->second<<std::endl;
                    throw std::runtime_error("error, locked offset is not locked");
                }
                uint32_t raw_offset = current_label_entry->delta_chain_index->at(cached_offset_it->first).get_raw_offset();
                if((raw_offset&LOCK_MASK)){
                    throw std::runtime_error("error, raw offset should not be locked");
                }
                if(cached_offset_it->second&LOCK_MASK){
                    throw std::runtime_error("error, cached offset should not be locked");
                }
#endif
                validated_offset_cache.first->second.emplace_back(cached_offset_it->first,current_label_entry->delta_chain_index->at(cached_offset_it->first).get_raw_offset());//cache the to-restore value without the lock, if we need to restore, release the lock together
                current_label_entry->delta_chain_index->at(cached_offset_it->first).update_offset(cached_offset_it->second);//set to my latest delta offset without lock, the in_progress delta will be the lock
            }
            BlockStateVersionProtectionScheme::release_protection(thread_id, block_access_ts_table);
            touched_block_it++;
        }
        //otherwise txn has to wait here, avoid deadlock
    }
    //if we made to read validation phase, we have succesfully block all edge deltas block to be consolidated, we can safely do validation.
    if(!to_abort){
        //validate read set
        //validate adjacency list
        for(auto it : scanned_adjacency_list){
            auto current_label_entry = get_label_entry(it);
            auto validation_access_result = BlockStateVersionProtectionScheme::committer_aborter_access_block(thread_id,it,current_label_entry,block_access_ts_table);
            if(validation_access_result == EdgeDeltaBlockState::NORMAL){
                auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(current_label_entry->block_ptr);
                uint64_t current_combined_offset = current_block->get_current_offset();
                if(current_block->is_overflow_offset(current_combined_offset)){
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    to_abort = true;
                    break;
                }
                auto delta_offset = current_block->get_delta_offset_from_combined_offset(current_combined_offset);
                auto edge_delta = current_block->get_edge_delta(delta_offset);
                auto latest_ts = edge_delta->creation_ts.load(std::memory_order_acquire);
                if(latest_ts>read_timestamp||latest_ts==0){
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    to_abort = true;
                    break;
                }
            }else{
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                to_abort = true;
                break;
            }
        }
        //validate individual edge
        for(auto it : read_set){
            auto current_label_entry = get_label_entry(it.first);
            auto validation_access_result = BlockStateVersionProtectionScheme::committer_aborter_access_block(thread_id,it.first,current_label_entry,block_access_ts_table);
            if(validation_access_result == EdgeDeltaBlockState::NORMAL){
                auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(current_label_entry->block_ptr);
                uint64_t current_combined_offset = current_block->get_current_offset();
                if(current_block->is_overflow_offset(current_combined_offset)){
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    to_abort = true;
                    break;
                }
                //validate edges
                for(auto itit : it.second){
                    auto delta_chain_id = current_block->get_delta_chain_id(itit);
                    auto& delta_chain_index_entry = current_label_entry->delta_chain_index->at(delta_chain_id);
                    auto index_offset = delta_chain_index_entry.get_offset();
                    //if locked, then current group or future group is updating it. current group txn will be ordered after us in the serial order if there is no dependency cycle, otherwise it will fail validation
                    //because we finish delta chain installation first. The other will abort so we are safe. But need to check the head to see if others have updated and committed it already.
                    //it is impossible for a locked delta chain pointing to an in-progress delta
                    if((index_offset>>31)>0)[[unlikely]]{
                        auto raw_offset = index_offset&UNLOCK_MASK;
                        if(raw_offset>0){
                            //still check the head delta, abort if it was updated
                            auto target_delta = current_block->get_edge_delta(raw_offset);
                            auto original_ts = target_delta->creation_ts.load(std::memory_order_acquire);
                            if(original_ts>read_timestamp){
                                to_abort = true;
                                break;
                            }
                        }
                    }else{
                        //if unlocked, check if it was updated. We also fail read validation if transactions in our group is updating it for safety.
                        auto target_delta = current_block->get_edge_delta(index_offset);
                        auto original_ts = target_delta->creation_ts.load(std::memory_order_acquire);
                        if(original_ts!=local_txn_id&&original_ts>read_timestamp){
                            to_abort = true;
                            break;
                        }
                    }
                }
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                if(to_abort){
                    break;
                }
            }else{
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                to_abort = true;
                break;
            }
        }
    }
    if(to_abort){
        for(auto reverse_it = already_validated_offsets_per_block.begin(); reverse_it!= already_validated_offsets_per_block.end();){
            if(reverse_it->second.empty()){
                reverse_it++;
                continue;
            }
            auto current_label_entry = get_label_entry(reverse_it->first);
            auto validation_state = BlockStateVersionProtectionScheme::committer_aborter_access_block(thread_id,reverse_it->first,current_label_entry,block_access_ts_table);
            if(validation_state==EdgeDeltaBlockState::NORMAL||validation_state==EdgeDeltaBlockState::CONSOLIDATION){
                auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(current_label_entry->block_ptr);
                auto& to_reverse_offsets = reverse_it->second;
                for(size_t i=0; i<to_reverse_offsets.size();i++){
                    auto& entry = to_reverse_offsets.at(i);
                    //restore offset and release lock
                    current_label_entry->delta_chain_index->at(entry.delta_chain_id).update_offset(entry.original_offset);//todo: original offset (in the index) should always be lock at the current design, so we release the lock and restore offset together
                }
                auto validated_offsets_cache = per_block_cached_delta_chain_offsets.find(reverse_it->first);
                if(validated_offsets_cache->second.is_outdated(current_label_entry->block_version_number.load(std::memory_order_acquire))){
                    throw DeltaChainOffsetException();
                }
                //lock released in updating offsets already
                op_count -= validated_offsets_cache->second.eager_abort(current_block,current_label_entry,local_txn_id,0);//must use cache
                per_block_cached_delta_chain_offsets.erase(validated_offsets_cache);
                reverse_it++;
            }else if(validation_state== EdgeDeltaBlockState::INSTALLATION
                     #if ENABLE_VERTEX_DELETION
                     ||validation_state==EdgeDeltaBlockState::DELETION
#endif
                    ){//mutex state already entered, so we will move forward, let consolidation thread/deletion thread observes our state and abort our deltas
                per_block_cached_delta_chain_offsets.erase(reverse_it->first);
                reverse_it++;
                continue;
            }else if(validation_state == EdgeDeltaBlockState::OVERFLOW){
                continue;//overflow state is transient so we can just come back later to valdiate in another state
            }else{
                //error state
                throw ValidationException();
            }
        }
        return false;
    }
    return true;
}

bool RWTransaction::commit() {
#if TRACK_EXECUTION_TIME
    auto start = std::chrono::high_resolution_clock::now();
#endif
#if LAZY_LOCKING
    if(!validation()){
        eager_abort();
        txn_tables.abort_txn(self_entry,op_count);//no need to cache the touched blocks of aborted txns due to eager abort
        return false;
    }
#else
    if(!simple_validation()){
        eager_abort();
        txn_tables.abort_txn(self_entry,op_count);//no need to cache the touched blocks of aborted txns due to eager abort
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        graph.local_thread_abort_time.local()+= duration.count();
#endif
        batch_lazy_updates();
        return false;
    }
#endif //LAZY_LOCKING
    for(auto it = per_block_cached_delta_chain_offsets.begin(); it!=per_block_cached_delta_chain_offsets.end();it++){
        //self_entry->touched_blocks.emplace_back(it->first, it->second.block_version_num);//safe because our validated blocks will not get version changed until we commit.
    }
    for(auto it = updated_vertices.begin(); it!=updated_vertices.end();it++){
        // self_entry->touched_blocks.emplace_back(*it,0);
    }
    self_entry->op_count.store(op_count,std::memory_order_release);
    commit_manager.txn_commit(thread_id,self_entry,true);//now do it simple, just wait
    batch_lazy_updates();
#if TRACK_EXECUTION_TIME
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    graph.local_thread_commit_time.local()+= duration.count();
#endif
    return true;
}

void RWTransaction::eager_clean_edge_block(uint64_t block_id, GTX::LockOffsetCache &validated_offsets) {
    timestamp_t commit_ts = self_entry->status.load(std::memory_order_acquire);
    auto target_label_entry = get_label_entry(block_id);
    //if we can access the block, we will do it and eager commit
    //if the block is under mutual exclusion state (consolidation), never mind, wait for lazy update
    if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry, block_access_ts_table)){
        if(validated_offsets.is_outdated(target_label_entry->block_version_number.load(std::memory_order_release)))[[unlikely]]{
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return;
        }
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        for(auto it = validated_offsets.already_updated_delta_chain_head_offsets.begin(); it!= validated_offsets.already_updated_delta_chain_head_offsets.end();it++){
            //clean up this offset
            uint32_t current_offset = it->second;
            auto current_delta = current_block->get_edge_delta(current_offset);
            timestamp_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
            while(current_offset>0&&(original_ts==local_txn_id||original_ts==commit_ts)){
#if USING_PREFETCH
                //__builtin_prefetch((const void*)current_block->get_edge_delta(current_delta->previous_offset),0,0);
                _mm_prefetch((const void*)current_block->get_edge_delta(current_delta->previous_offset), _MM_HINT_T2);
#endif
                if(original_ts==local_txn_id){
                    current_block->update_previous_delta_invalidate_ts(current_delta->toID, current_delta->previous_version_offset, commit_ts);
                    if(current_delta->lazy_update(original_ts,commit_ts)){
                        self_entry->op_count.fetch_sub(1,std::memory_order_acq_rel);
                    }
                }
                current_offset = current_delta->previous_offset;
                current_delta = current_block->get_edge_delta(current_offset);
                original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
            }
        }
        BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
    }
}

void RWTransaction::eager_clean_vertex_chain(GTX::vertex_t vid) {
    timestamp_t commit_ts = self_entry->status.load(std::memory_order_acquire);
    auto& index_entry = graph.get_vertex_index_entry(vid);//get vertex index entry
    auto vertex_delta = block_manager.convert<VertexDeltaHeader>(index_entry.vertex_delta_chain_head_ptr);
    if(vertex_delta->lazy_update(local_txn_id,commit_ts)){
        self_entry->op_count.fetch_sub(1,std::memory_order_acq_rel);
    }
}

bool RWTransaction::eager_commit() {
#if TRACK_EXECUTION_TIME
    auto start = std::chrono::high_resolution_clock::now();
#endif
    self_entry->op_count.store(op_count,std::memory_order_release);
    //commit_manager.txn_commit(thread_id,self_entry,true);//now do it simple, just wait
    //read set validation
    batch_lazy_updates();
    commit_manager.register_validation_group(thread_id,self_entry);//only the current validation group can move forward
#if LAZY_LOCKING
    if(!validation()){
        eager_abort();
        txn_tables.abort_txn(self_entry,op_count);//no need to cache the touched blocks of aborted txns due to eager abort
        return false;
    }
#else
    if(!simple_validation())[[unlikely]]{
        commit_manager.finish_validation(thread_id,true);
        eager_abort();
        txn_tables.abort_txn(self_entry,op_count);//no need to cache the touched blocks of aborted txns due to eager abort
        batch_lazy_updates();
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        graph.local_thread_abort_time.local()+= duration.count();
        auto txn_lifetime = std::chrono::duration_cast<std::chrono::microseconds>(stop - txn_start_time);
        graph.txn_execution_time.local()+= txn_lifetime.count();
#endif
#if TRACK_COMMIT_ABORT
        graph.register_abort();
#endif
        self_entry->validating.store(false);
        return false;
    }
#endif //LAZY_LOCKING
//#if USING_COMMIT_WAIT_WORK
    /*  if(!self_entry->status.load())[[likely]]{
          eager_garbage_collection();
          while(!self_entry->status.load(std::memory_order_acquire));
      }*/
//#else
//even with wal, txn status won't get updated until persisted wal
    commit_manager.finish_validation(thread_id,false);
    while(!self_entry->status.load(std::memory_order_acquire));//loop until committed, fixme: it seems to be a bottleneck, spent 6% of CPU
    //self_entry->status.wait(IN_PROGRESS,std::memory_order_acquire);
//#endif //USING_COMMIt_WAIT_WORK
    //while()
    //eager clean
    for(auto it = per_block_cached_delta_chain_offsets.begin(); it!=per_block_cached_delta_chain_offsets.end();it++){
        eager_clean_edge_block(it->first,it->second);
    }
    for(auto it = updated_vertices.begin(); it!=updated_vertices.end();it++){
        eager_clean_vertex_chain(*it);
    }
#if TRACK_EXECUTION_TIME
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    graph.local_thread_commit_time.local()+= duration.count();
    auto txn_lifetime = std::chrono::duration_cast<std::chrono::microseconds>(stop - txn_start_time);
    graph.txn_execution_time.local()+= txn_lifetime.count();
#endif
#if TRACK_COMMIT_ABORT
    graph.register_commit();
#endif
    self_entry->validating.store(false);
    return true;
}

#if ENSURE_DURABILITY
void RWTransaction::record_wal(GTX::WALType type, GTX::vertex_t src, std::string_view data, GTX::vertex_t dst,
                               GTX::label_t label) {
    std::string log;
    /*
    if(type ==WALType::EDGE_UPDATE){
        log.append("eu");
        log.append(",");
        log.append(std::to_string(label));
        log.append(",");
        log.append(std::to_string(src));
        log.append(",");
        log.append(std::to_string(dst));
        log.append(",");
        log.append(data);
        //log.append(std::to_string(*reinterpret_cast<double*>(data.data())));
        log.append(";");
    }else if(type ==WALType::EDGE_DELETE){
        log.append("ed");
        log.append(",");
        log.append(std::to_string(label));
        log.append(",");
        log.append(std::to_string(src));
        log.append(",");
        log.append(std::to_string(dst));
        log.append(";");
    }else if(type == WALType::VERTEX_UPDATE){
        log.append("vu");
        log.append(",");
        log.append(std::to_string(src));
        log.append(",");
        log.append(data);
        //log.append(std::to_string(*reinterpret_cast<uint64_t*>(data.data())));
        log.append(";");
    }
    else{
        log.append("vd");
        log.append(",");
        log.append(std::to_string(src));
        log.append(";");
    }*/
    if(type ==WALType::EDGE_UPDATE){
        log.append("eu");
        //log.append(",");
        log.append(std::string_view((char*)(&label),sizeof (label)));
        //log.append(",");
        log.append(std::string_view((char*)(&src),sizeof (src)));
        //log.append(",");
        log.append(std::string_view((char*)(&dst),sizeof (dst)));
        //log.append(",");
        log.append(data);
        //log.append(std::to_string(*reinterpret_cast<double*>(data.data())));
        //log.append(";");
    }else if(type ==WALType::EDGE_DELETE){
        log.append("ed");
        //log.append(",");
        log.append(std::string_view((char*)(&label),sizeof (label)));
        //log.append(",");
        log.append(std::string_view((char*)(&src),sizeof (src)));
        //log.append(",");
        log.append(std::string_view((char*)(&dst),sizeof (dst)));
        //log.append(";");
    }else if(type == WALType::VERTEX_UPDATE){
        log.append("vu");
        //log.append(",");
        log.append(std::string_view((char*)(&src),sizeof (src)));
        //log.append(",");
        log.append(data);
        //log.append(std::to_string(*reinterpret_cast<uint64_t*>(data.data())));
        //log.append(";");
    }
    else{
        log.append("vd");
        //log.append(",");
        log.append(std::string_view((char*)(&src),sizeof (src)));
        //log.append(";");
    }
    self_entry->get_wal().append(log);
}
#endif

void RWTransaction::abort() {
#if TRACK_EXECUTION_TIME
    auto start = std::chrono::high_resolution_clock::now();
#endif
    batch_lazy_updates();
    //eager abort no need to cache
    eager_abort();
    txn_tables.abort_txn(self_entry,op_count);//no need to cache the touched blocks of aborted txns due to eager abort
#if TRACK_EXECUTION_TIME
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    graph.local_thread_abort_time.local()+= duration.count();
    auto txn_lifetime = std::chrono::duration_cast<std::chrono::microseconds>(stop - txn_start_time);
    graph.txn_execution_time.local()+=txn_lifetime.count();
#endif
}


//vertex operations
//maybe txn eager abort will recycle allocated vertices.
vertex_t RWTransaction::create_vertex() {
#if TRACK_EXECUTION_TIME
    auto start = std::chrono::high_resolution_clock::now();
#endif
    //todo: further design the reuse vertex part, currently assume no vertex deletion
    auto& vertex_index = graph.get_vertex_index();
    if(!thread_local_recycled_vertices.empty()){
        auto reuse_vid = thread_local_recycled_vertices.front();
#if TXN_TEST
        auto& vertex_index_entry = vertex_index.get_vertex_index_entry(reuse_vid);
        if(vertex_index_entry.valid.load(std::memory_order_acquire)){
            throw VertexCreationException();
        }
#endif
        thread_local_recycled_vertices.pop();
        vertex_index.make_valid(reuse_vid);
        created_vertices.emplace(reuse_vid);

        //vertex_index.make_valid(reuse_vid); already valid, but just deleted or no delta at all
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        graph.local_thread_vertex_write_time.local()+= duration.count();
#endif
        return reuse_vid;
    }
    auto new_vid = vertex_index.get_next_vid();
    auto& vertex_index_entry = vertex_index.get_vertex_index_entry(new_vid);
#if TXN_TEST
    if(vertex_index_entry.valid.load(std::memory_order_acquire)){
        throw VertexCreationException();
    }
#endif
    //allocate an initial label block
    vertex_index_entry.valid.store(true,std::memory_order_release);
    vertex_index_entry.edge_label_block_ptr = block_manager.alloc(size_to_order(sizeof(EdgeLabelBlock)));
    auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
    edge_label_block->fill_information(new_vid,&block_manager);//allocate and fill in new initial label block
    //vertex_index.make_valid(new_vid);
    created_vertices.emplace(new_vid);
#if TRACK_EXECUTION_TIME
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    graph.local_thread_vertex_write_time.local()+= duration.count();
#endif
    return new_vid;
}

//can be updating a new version or delete
//todo: make sure if we encounter a deleted vertex, we first abort and wait till the vertex is recycled
Txn_Operation_Response RWTransaction::update_vertex(GTX::vertex_t src, std::string_view vertex_data) {
    auto& vertex_index_entry = graph.get_vertex_index_entry(src);
    if(!vertex_index_entry.valid.load(std::memory_order_acquire)){
        throw IllegalVertexAccessException();
    }
    uintptr_t current_vertex_delta_ptr;
    VertexDeltaHeader* current_vertex_delta =nullptr;
    timestamp_t original_ts = 0;
    while(true){
        current_vertex_delta_ptr = vertex_index_entry.vertex_delta_chain_head_ptr.load(std::memory_order_acquire);
        if(current_vertex_delta_ptr){
            current_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta_ptr);
            /*timestamp_t*/ original_ts = current_vertex_delta->get_creation_ts();
            if(original_ts==local_txn_id){
                break;
            }
            if(is_txn_id(original_ts)){
                uint64_t status;
                if(txn_tables.get_status(original_ts,status)){
                    if(status==IN_PROGRESS){
                        return Txn_Operation_Response::FAIL;
                    }else if(status!=ABORT){
                        if(current_vertex_delta->lazy_update(original_ts,status)){
                            record_lazy_update_record(&lazy_update_records,original_ts);
                            //invalidate previous entry if exist
                            if(current_vertex_delta->get_previous_ptr()){
                                auto previous_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta->get_previous_ptr());
                                per_thread_garbage_queue.register_entry(current_vertex_delta->get_previous_ptr(),previous_vertex_delta->get_order(),status);
#if !USING_COMMIT_WAIT_WORK
                                if(per_thread_garbage_queue.need_collection()){
                                    auto safe_ts = block_access_ts_table.calculate_safe_ts();
                                    per_thread_garbage_queue.free_block(safe_ts);
                                }
#endif
                            }
                        }
#if TXN_TEST
                        if(current_vertex_delta->get_creation_ts()!=status){
                            throw VertexDeltaException();
                        }
#endif
                        if(status>read_timestamp){
                            return Txn_Operation_Response::FAIL;
                        }else{
                            break;
                        }
                    }else{
                        continue; //status == abort, eager abort is done, we need to reread the entry
                    }
                }else{
                    continue;//no status so delta is lazy updated
                }
            }else if(original_ts!=ABORT){
                if(original_ts>read_timestamp){
                    return Txn_Operation_Response::FAIL;
                }
                break;
            }else{
                continue;//don't worry because of eager abort, continue and reload delta until we see a committed version or 0
            }
        }else{
            break;
        }
    }
    //either 0 or comitted current version
#if TXN_TEST
    if(current_vertex_delta_ptr){
        if(current_vertex_delta->get_creation_ts()!=local_txn_id){
            if(is_txn_id(current_vertex_delta->get_creation_ts())||current_vertex_delta->get_creation_ts()>read_timestamp){
                throw VertexDeltaException();
            }
        }
    }
#endif
    const char* data = vertex_data.data();
    order_t new_order = size_to_order(sizeof(VertexDeltaHeader)+vertex_data.size());
    auto new_delta_ptr = block_manager.alloc(new_order);
    auto new_vertex_delta = block_manager.convert<VertexDeltaHeader>(new_delta_ptr);
    //common case, initial vertex version creation, no update
    if(!current_vertex_delta_ptr){
        new_vertex_delta->fill_metadata(local_txn_id,vertex_data.size(),new_order,current_vertex_delta_ptr);
        new_vertex_delta->write_data(data);
        if(vertex_index_entry.install_vertex_delta(current_vertex_delta_ptr,new_delta_ptr)){
            updated_vertices.emplace(src);
            op_count++;
            return Txn_Operation_Response::SUCCESS;
        }else{
            auto zero_out_ptr = block_manager.convert<uint8_t>(new_delta_ptr);
            memset(zero_out_ptr,'\0',1ul<<new_order);
            block_manager.free(new_delta_ptr,new_order);
            return Txn_Operation_Response::FAIL;
        }
    }else{
        //updating a vertex version
        if(current_vertex_delta->get_creation_ts()!=local_txn_id){
            new_vertex_delta->fill_metadata(local_txn_id,vertex_data.size(),new_order,current_vertex_delta_ptr);
            new_vertex_delta->write_data(data);
            if(vertex_index_entry.install_vertex_delta(current_vertex_delta_ptr,new_delta_ptr)){
                updated_vertices.emplace(src);
                op_count++;
                return Txn_Operation_Response::SUCCESS;
            }else{
                auto zero_out_ptr = block_manager.convert<uint8_t>(new_delta_ptr);
                memset(zero_out_ptr,'\0',1ul<<new_order);
                block_manager.free(new_delta_ptr,new_order);
                return Txn_Operation_Response::FAIL;
            }
        }else{
            new_vertex_delta->fill_metadata(local_txn_id,vertex_data.size(),new_order,current_vertex_delta->get_previous_ptr());
            new_vertex_delta->write_data(data);
#if TXN_TEST
            if(!vertex_index_entry.install_vertex_delta(current_vertex_delta_ptr,new_delta_ptr)||updated_vertices.find(src)==updated_vertices.end()) {
                throw VertexDeltaException();
            }
#else
            vertex_index_entry.vertex_delta_chain_head_ptr.store(new_delta_ptr,std::memory_order_release);
#endif
            //updated_vertices.emplace(src); no need, updated_vertices should already contain it
            //op_count++; we are replacing our delta so no increase in op count, also garbage collect replaced entry
            per_thread_garbage_queue.register_entry(current_vertex_delta_ptr,current_vertex_delta->get_order(),commit_manager.get_current_read_ts());
#if !USING_COMMIT_WAIT_WORK
            if(per_thread_garbage_queue.need_collection()){
                auto safe_ts = block_access_ts_table.calculate_safe_ts();
                per_thread_garbage_queue.free_block(safe_ts);
            }
#endif
            return Txn_Operation_Response::SUCCESS;
        }
    }

}

std::string_view RWTransaction::get_vertex(GTX::vertex_t src) {
    auto& vertex_index_entry = graph.get_vertex_index_entry(src);
    if(!vertex_index_entry.valid.load(std::memory_order_acquire)){
        return std::string_view ();
        //throw IllegalVertexAccessException();
    }
    uintptr_t current_vertex_delta_ptr = vertex_index_entry.vertex_delta_chain_head_ptr.load(std::memory_order_acquire);
    if(!current_vertex_delta_ptr){
        return std::string_view ();
    }
    VertexDeltaHeader* current_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta_ptr);
    uint64_t current_ts = current_vertex_delta->get_creation_ts();
    if(current_ts==local_txn_id){
        char* data = current_vertex_delta->get_data();
        return std::string_view (data,current_vertex_delta->get_data_size());
    }
    if(is_txn_id(current_ts)){
        uint64_t status;
        if(txn_tables.get_status(current_ts,status)){
            if(status!=IN_PROGRESS){//ignore in progress ones
                if(status!=ABORT){//abort will be eager updated
                    if(current_vertex_delta->lazy_update(current_ts,status)){
                        record_lazy_update_record(&lazy_update_records,current_ts);
                        //invalidate previous entry if exist
                        if(current_vertex_delta->get_previous_ptr()){
                            auto previous_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta->get_previous_ptr());
                            per_thread_garbage_queue.register_entry(current_vertex_delta->get_previous_ptr(),previous_vertex_delta->get_order(),status);
#if !USING_COMMIT_WAIT_WORK
                            if(per_thread_garbage_queue.need_collection()){
                                auto safe_ts = block_access_ts_table.calculate_safe_ts();
                                per_thread_garbage_queue.free_block(safe_ts);
                            }
#endif
                        }
                    }
                }
            }
        }
    }
    while(current_vertex_delta_ptr){
        if(current_vertex_delta->get_creation_ts()<=read_timestamp){
            char* data = current_vertex_delta->get_data();
            return std::string_view (data,current_vertex_delta->get_data_size());
        }
        current_vertex_delta_ptr = current_vertex_delta->get_previous_ptr();
        current_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta_ptr);
    }
    return std::string_view();
}

RWTransaction::~RWTransaction()=default;/*{
    if(!lazy_update_records.empty()){
        std::cout<<"error, txn neither committed or aborted"<<std::endl;
        std::cout<<"txn id is "<<self_entry->txn_id<<" status is "<<self_entry->status<<" op count is "<<self_entry->op_count<<std::endl;
        batch_lazy_updates();
    }
}*/

std::pair<Txn_Operation_Response, std::string_view>
ROTransaction::get_edge(GTX::vertex_t src, GTX::vertex_t dst, GTX::label_t label) {
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry)[[unlikely]]{
        return std::pair<Txn_Operation_Response, std::string_view>(Txn_Operation_Response::SUCCESS,std::string_view());
    }
    auto block_id = generate_block_id(src,label);
    if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry,block_access_ts_table))[[likely]]{
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        if(read_timestamp>=current_block->get_creation_time())[[likely]]{
            auto& delta_chains_index_entry = target_label_entry->delta_chain_index->at(current_block->get_delta_chain_id(dst));
            uint32_t offset = delta_chains_index_entry.get_raw_offset();//may be locked
            BaseEdgeDelta* target_delta = nullptr;

            if(offset){
                if(offset<=ENTRY_DELTA_SIZE*8){
                    //current_block->scan_prefetch(offset);
                    target_delta = current_block->get_visible_target_using_scan(offset,dst,read_timestamp,lazy_update_records);
                }else{
                    target_delta =current_block->get_visible_target_delta_using_delta_chain(offset,dst,read_timestamp,lazy_update_records);
                }
            }
            if(target_delta)[[likely]]{
                char* data;
                if(target_delta->data_length<=16){
                    data= target_delta->data;
                }else{
                    data = current_block->get_edge_data(target_delta->data_offset);
                }
#if EDGE_DELTA_TEST
                if(target_delta->toID!=dst){
                    throw TransactionReadException();
                }
#endif
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::SUCCESS, std::string_view(data,target_delta->data_length));
            }else{
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::SUCCESS,std::string());
            }

        }else{
#if EDGE_DELTA_TEST
            if(!current_block->get_previous_ptr()){
                throw TransactionReadException();
            }
#endif
            EdgeDeltaBlockHeader* previous_block = block_manager.convert<EdgeDeltaBlockHeader>(current_block->get_previous_ptr());
            while(read_timestamp<previous_block->get_creation_time()){
#if EDGE_DELTA_TEST
                if(!previous_block->get_previous_ptr()){
                    throw TransactionReadException();
                }
#endif
                previous_block = block_manager.convert<EdgeDeltaBlockHeader>(previous_block->get_previous_ptr());
            }
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);//reading previous block needs no protection, it is protected by read epoch
            return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::SUCCESS,
                                                                      scan_previous_block_find_edge(previous_block,dst));
        }
    }else{
#if ENABLE_VERTEX_DELETION
        auto current_state = target_label_entry->state.load(std::memory_order_acquire);
        if(current_state==EdgeDeltaBlockState::DELETION)[[unlikely]]{
            return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::FAIL,std::string_view());
        }
#endif
        return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::READER_WAIT,std::string_view());
    }
}

Txn_Operation_Response
ROTransaction::get_edge_weight(GTX::vertex_t src, GTX::label_t label, GTX::vertex_t dst, double *&weight) {
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry)[[unlikely]]{
        return Txn_Operation_Response::SUCCESS;
    }
    auto block_id = generate_block_id(src,label);
    if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry,block_access_ts_table))[[likely]]{
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        if(read_timestamp>=current_block->get_creation_time())[[likely]]{
            auto& delta_chains_index_entry = target_label_entry->delta_chain_index->at(current_block->get_delta_chain_id(dst));
            uint32_t offset = delta_chains_index_entry.get_raw_offset();//may be locked
            BaseEdgeDelta* target_delta = nullptr;

            if(offset){
                if(offset<=ENTRY_DELTA_SIZE*8){
                    target_delta = current_block->get_visible_target_using_scan(offset,dst,read_timestamp,lazy_update_records);
                }else{
                    target_delta =current_block->get_visible_target_delta_using_delta_chain(offset,dst,read_timestamp,lazy_update_records);
                }
            }
            if(target_delta)[[likely]]{
                weight = reinterpret_cast<double*>(target_delta->data);
#if EDGE_DELTA_TEST
                if(target_delta->toID!=dst){
                    throw TransactionReadException();
                }
#endif
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::SUCCESS;
            }else{
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return Txn_Operation_Response::SUCCESS;
            }

        }else{
#if EDGE_DELTA_TEST
            if(!current_block->get_previous_ptr()){
                throw TransactionReadException();
            }
#endif
            EdgeDeltaBlockHeader* previous_block = block_manager.convert<EdgeDeltaBlockHeader>(current_block->get_previous_ptr());
            while(read_timestamp<previous_block->get_creation_time()){
#if EDGE_DELTA_TEST
                if(!previous_block->get_previous_ptr()){
                    throw TransactionReadException();
                }
#endif
                previous_block = block_manager.convert<EdgeDeltaBlockHeader>(previous_block->get_previous_ptr());
            }
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);//reading previous block needs no protection, it is protected by read epoch
            scan_previous_block_find_weight(previous_block,dst,weight);
            return Txn_Operation_Response::SUCCESS;
        }
    }else{
#if ENABLE_VERTEX_DELETION
        auto current_state = target_label_entry->state.load(std::memory_order_acquire);
        if(current_state==EdgeDeltaBlockState::DELETION)[[unlikely]]{
            return Txn_Operation_Response::FAIL;
        }
#endif
        return Txn_Operation_Response::READER_WAIT;
    }
}

std::pair<Txn_Operation_Response, EdgeDeltaIterator>
ROTransaction::get_edges(GTX::vertex_t src, GTX::label_t label) {
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry){
        return std::pair<Txn_Operation_Response, EdgeDeltaIterator>(Txn_Operation_Response::SUCCESS,EdgeDeltaIterator());
    }
    auto block_id = generate_block_id(src, label);
    if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry,block_access_ts_table)){
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        uint64_t current_combined_offset = current_block->get_current_offset();
        if(current_block->is_overflow_offset(current_combined_offset)){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return std::pair<Txn_Operation_Response,EdgeDeltaIterator>(Txn_Operation_Response::READER_WAIT,EdgeDeltaIterator());
        }
        //block protection released by iterator destructor
        return std::pair<Txn_Operation_Response,EdgeDeltaIterator>(Txn_Operation_Response::SUCCESS,EdgeDeltaIterator(current_block,read_timestamp,((static_cast<uint64_t>(thread_id)<<56)|placeholder_txn_id),false,EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset),
                                                                                                                     graph,&lazy_update_records,&block_access_ts_table));
    }else{
#if ENABLE_VERTEX_DELETION
        auto current_state = target_label_entry->state.load(std::memory_order_acquire);
        if(current_state==EdgeDeltaBlockState::DELETION)[[unlikely]]{
            return std::pair<Txn_Operation_Response,EdgeDeltaIterator>(Txn_Operation_Response::FAIL,EdgeDeltaIterator());
        }
#endif
        return std::pair<Txn_Operation_Response, EdgeDeltaIterator>(Txn_Operation_Response::READER_WAIT,EdgeDeltaIterator());
    }
}

std::pair<Txn_Operation_Response, SimpleEdgeDeltaIterator>
ROTransaction::simple_get_edges(GTX::vertex_t src, GTX::label_t label) {
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry){
        return std::pair<Txn_Operation_Response, SimpleEdgeDeltaIterator>(Txn_Operation_Response::SUCCESS,SimpleEdgeDeltaIterator());
    }
    auto block_id = generate_block_id(src, label);
    if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry,block_access_ts_table)){
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        uint64_t current_combined_offset = current_block->get_current_offset();
        if(current_block->is_overflow_offset(current_combined_offset)){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator>(Txn_Operation_Response::READER_WAIT,SimpleEdgeDeltaIterator());
        }
        //block protection released by iterator close
        return std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator>(Txn_Operation_Response::SUCCESS,SimpleEdgeDeltaIterator(current_block,read_timestamp,((static_cast<uint64_t>(thread_id)<<56)|placeholder_txn_id),EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset),
                                                                                                                                 graph,&lazy_update_records,&block_access_ts_table));
    }else{
#if ENABLE_VERTEX_DELETION
        auto current_state = target_label_entry->state.load(std::memory_order_acquire);
        if(current_state==EdgeDeltaBlockState::DELETION)[[unlikely]]{
            return std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator>(Txn_Operation_Response::FAIL,SimpleEdgeDeltaIterator());
        }
#endif
        return std::pair<Txn_Operation_Response, SimpleEdgeDeltaIterator>(Txn_Operation_Response::READER_WAIT,SimpleEdgeDeltaIterator());
    }
}

std::string_view ROTransaction::get_vertex(GTX::vertex_t src) {
    auto& vertex_index_entry = graph.get_vertex_index_entry(src);
    if(!vertex_index_entry.valid.load(std::memory_order_acquire)){
        return std::string_view ();
        //throw IllegalVertexAccessException();
    }
    uintptr_t current_vertex_delta_ptr = vertex_index_entry.vertex_delta_chain_head_ptr.load(std::memory_order_acquire);
    if(!current_vertex_delta_ptr){
        return std::string_view ();
    }
    VertexDeltaHeader* current_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta_ptr);
    uint64_t current_ts = current_vertex_delta->get_creation_ts();
    if(is_txn_id(current_ts)){
        uint64_t status;
        if(txn_tables.get_status(current_ts,status)){
            if(status!=IN_PROGRESS){//ignore in progress ones
                if(status!=ABORT){//abort will be eager updated
                    if(current_vertex_delta->lazy_update(current_ts,status)){
                        record_lazy_update_record(&lazy_update_records,current_ts);
                        //invalidate previous entry if exist
                        if(current_vertex_delta->get_previous_ptr()){
                            auto previous_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta->get_previous_ptr());
                            per_thread_garbage_queue.register_entry(current_vertex_delta->get_previous_ptr(),previous_vertex_delta->get_order(),status);
#if !USING_COMMIT_WAIT_WORK
                            if(per_thread_garbage_queue.need_collection()){
                                auto safe_ts = block_access_ts_table.calculate_safe_ts();
                                per_thread_garbage_queue.free_block(safe_ts);
                            }
#endif
                        }
                    }
                }
            }
        }
    }
    while(current_vertex_delta_ptr){
        if(current_vertex_delta->get_creation_ts()<=read_timestamp){
            char* data = current_vertex_delta->get_data();
            return std::string_view (data,current_vertex_delta->get_data_size());
        }
        current_vertex_delta_ptr = current_vertex_delta->get_previous_ptr();
        current_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta_ptr);
    }
    return std::string_view();
}
//executed with no block state protection
std::string_view
ROTransaction::scan_previous_block_find_edge(GTX::EdgeDeltaBlockHeader *previous_block, GTX::vertex_t vid) {
    uint64_t combined_offset = previous_block->get_current_offset();
    uint32_t current_delta_offset = EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(combined_offset);
    BaseEdgeDelta* current_delta = previous_block->get_edge_delta(current_delta_offset);
    while(current_delta_offset>0){
        if(current_delta->toID==vid){
            uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
            //abort and txn id are both quite large
            if(original_ts<=read_timestamp){
                if(current_delta->data_length<=16){
                    return std::string_view (current_delta->data,current_delta->data_length);
                }else{
                    return std::string_view (previous_block->get_edge_data(current_delta->data_offset),current_delta->data_length);
                }
            }
        }
        current_delta ++;
        current_delta_offset-=ENTRY_DELTA_SIZE;
    }
    return std::string_view();
}

void
ROTransaction::scan_previous_block_find_weight(GTX::EdgeDeltaBlockHeader *previous_block, GTX::vertex_t vid,
                                               double *&weight) {
    uint64_t combined_offset = previous_block->get_current_offset();
    uint32_t current_delta_offset = EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(combined_offset);
    BaseEdgeDelta* current_delta = previous_block->get_edge_delta(current_delta_offset);
    while(current_delta_offset>0){
        if(current_delta->toID==vid){
            uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
            //abort and txn id are both quite large
            if(original_ts<=read_timestamp)[[likely]]{
                weight = reinterpret_cast<double*>(current_delta->data);
                return;
            }
        }
        current_delta ++;
        current_delta_offset-=ENTRY_DELTA_SIZE;
    }
}

ROTransaction::~ROTransaction() = default;

SharedROTransaction::~SharedROTransaction() = default;

std::string_view
SharedROTransaction::static_get_edge(GTX::vertex_t src, GTX::vertex_t dst, GTX::label_t label) {
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry){
        return std::string_view();
    }
    auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
    auto& delta_chains_index_entry = target_label_entry->delta_chain_index->at(current_block->get_delta_chain_id(dst));
    uint32_t offset = delta_chains_index_entry.get_offset();//static graph, must already be loaded
    BaseEdgeDelta* target_delta =current_block->get_visible_target_delta_using_delta_chain(offset,dst,read_timestamp, thread_specific_lazy_update_records.local());
    if(!target_delta){
        return std::string_view();
    }else{
        char* data;
        if(target_delta->data_length<=16){
            data = target_delta->data;
        }else{
            data = current_block->get_edge_data(target_delta->data_offset);
        }
#if EDGE_DELTA_TEST
        if(target_delta->toID!=dst){
                    throw TransactionReadException();
                }
#endif
        return std::string_view(data,target_delta->data_length);
    }
}

SimpleEdgeDeltaIterator SharedROTransaction::generate_edge_iterator(uint8_t thread_id) {
    return {&txn_tables,&block_manager,&thread_specific_lazy_update_records.local(),&block_access_ts_table,read_timestamp,((static_cast<uint64_t>(thread_id)<<56)|placeholder_txn_id)};
    //& thread_specific_lazy_update_records.local()
}

StaticEdgeDeltaIterator SharedROTransaction::generate_static_edge_iterator() {
    return {};
}
//read operations for static graph
StaticEdgeDeltaIterator SharedROTransaction::static_get_edges(GTX::vertex_t src, GTX::label_t label) {
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry)[[unlikely]]{
        return StaticEdgeDeltaIterator();
    }
    auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
    uint64_t current_combined_offset = current_block->get_current_offset();
#if USING_PREFETCH
    /*   uint32_t current_delta_offset = EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset);
       uint32_t num = current_delta_offset/ENTRY_DELTA_SIZE;
       num = (num<10)? num:10;
       for(uint32_t i=0; i<num; i++){
           _mm_prefetch((const void*)(current_block->get_edge_delta(current_delta_offset-i*ENTRY_DELTA_SIZE)),_MM_HINT_T2);
       }*/
#endif
    return StaticEdgeDeltaIterator(current_block, EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset));
}
void SharedROTransaction::static_get_edges(vertex_t src, label_t label, std::unique_ptr<StaticEdgeDeltaIterator>& edge_iterator){
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry)[[unlikely]]{
        return;
    }
    auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
    uint64_t current_combined_offset = current_block->get_current_offset();
    edge_iterator.get()->fill_information(current_block,EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset));
}
std::string_view SharedROTransaction::static_get_vertex(GTX::vertex_t src) {
    auto& vertex_index_entry = graph.get_vertex_index_entry(src);
    if(!vertex_index_entry.valid.load(std::memory_order_acquire))[[unlikely]]{
        return std::string_view ();
        //throw IllegalVertexAccessException();
    }
    uintptr_t current_vertex_delta_ptr = vertex_index_entry.vertex_delta_chain_head_ptr.load(std::memory_order_acquire);
    if(!current_vertex_delta_ptr)[[unlikely]]{
        return std::string_view ();
    }
    VertexDeltaHeader* current_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta_ptr);
    char* data = current_vertex_delta->get_data();
    return std::string_view (data,current_vertex_delta->get_data_size());
}
/*
 * its usage should be limited. We'd like openmp thread to know its ID before invoking the function
 */
//todo: add support to not return deleted vertex
std::string_view SharedROTransaction::get_vertex(GTX::vertex_t src) {
    auto& vertex_index_entry = graph.get_vertex_index_entry(src);
    if(!vertex_index_entry.valid.load(std::memory_order_acquire)){
        return std::string_view ();
        //throw IllegalVertexAccessException();
    }
    uintptr_t current_vertex_delta_ptr = vertex_index_entry.vertex_delta_chain_head_ptr.load(std::memory_order_acquire);
    if(!current_vertex_delta_ptr){
        return std::string_view ();
    }
    VertexDeltaHeader* current_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta_ptr);
    uint64_t current_ts = current_vertex_delta->get_creation_ts();
    if(is_txn_id(current_ts)){
        uint64_t status;
        if(txn_tables.get_status(current_ts,status)){
            if(status!=IN_PROGRESS){//ignore in progress ones
                if(status!=ABORT){//abort will be eager updated
                    if(current_vertex_delta->lazy_update(current_ts,status)){
                        record_lazy_update_record(& thread_specific_lazy_update_records.local(),current_ts);
                        //invalidate previous entry if exist
                        if(current_vertex_delta->get_previous_ptr()){
                            auto previous_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta->get_previous_ptr());
                            uint8_t thread_id = graph.get_worker_thread_id();
                            graph.get_per_thread_garbage_queue(thread_id).register_entry(current_vertex_delta->get_previous_ptr(),previous_vertex_delta->get_order(),status);
#if !USING_COMMIT_WAIT_WORK
                            if(graph.get_per_thread_garbage_queue(thread_id).need_collection()){
                                auto safe_ts = block_access_ts_table.calculate_safe_ts();
                                graph.get_per_thread_garbage_queue(thread_id).free_block(safe_ts);
                            }
#endif
                        }
                    }
                }
            }
        }
    }
    while(current_vertex_delta_ptr){
        if(current_vertex_delta->get_creation_ts()<=read_timestamp){
            char* data = current_vertex_delta->get_data();
            return std::string_view (data,current_vertex_delta->get_data_size());
        }
        current_vertex_delta_ptr = current_vertex_delta->get_previous_ptr();
        current_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta_ptr);
    }
    return std::string_view();
}
//todo: add support to not return deleted vertex
std::string_view SharedROTransaction::get_vertex(GTX::vertex_t src, uint8_t thread_id) {
    auto& vertex_index_entry = graph.get_vertex_index_entry(src);
    if(!vertex_index_entry.valid.load(std::memory_order_acquire))[[unlikely]]{
        return std::string_view ();
        //throw IllegalVertexAccessException();
    }
    uintptr_t current_vertex_delta_ptr = vertex_index_entry.vertex_delta_chain_head_ptr.load(std::memory_order_acquire);
    if(!current_vertex_delta_ptr)[[unlikely]]{
        return std::string_view ();
    }
    VertexDeltaHeader* current_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta_ptr);
    uint64_t current_ts = current_vertex_delta->get_creation_ts();
    if(is_txn_id(current_ts))[[unlikely]]{
        uint64_t status;
        if(txn_tables.get_status(current_ts,status)){
            if(status!=IN_PROGRESS){//ignore in progress ones
                if(status!=ABORT){//abort will be eager updated
                    if(current_vertex_delta->lazy_update(current_ts,status)){
                        record_lazy_update_record(& thread_specific_lazy_update_records.local(),current_ts);
                        //invalidate previous entry if exist
                        if(current_vertex_delta->get_previous_ptr()){
                            auto previous_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta->get_previous_ptr());
                            graph.get_per_thread_garbage_queue(thread_id).register_entry(current_vertex_delta->get_previous_ptr(),previous_vertex_delta->get_order(),status);
#if !USING_COMMIT_WAIT_WORK
                            if(graph.get_per_thread_garbage_queue(thread_id).need_collection()){
                                auto safe_ts = block_access_ts_table.calculate_safe_ts();
                                graph.get_per_thread_garbage_queue(thread_id).free_block(safe_ts);
                            }
#endif
                        }
                    }
                }
            }
        }
    }
    while(current_vertex_delta_ptr){
        if(current_vertex_delta->get_creation_ts()<=read_timestamp)[[likely]]{
            char* data = current_vertex_delta->get_data();
            return std::string_view (data,current_vertex_delta->get_data_size());
        }
        current_vertex_delta_ptr = current_vertex_delta->get_previous_ptr();
        current_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta_ptr);
    }
    return std::string_view();
}

#if ENSURE_DURABILITY

VertexDeltaHeader *SharedROTransaction::get_vertex_delta(GTX::vertex_t src, uint8_t thread_id) {
    auto& vertex_index_entry = graph.get_vertex_index_entry(src);
    if(!vertex_index_entry.valid.load(std::memory_order_acquire))[[unlikely]]{
        return nullptr;
    }
    uintptr_t current_vertex_delta_ptr = vertex_index_entry.vertex_delta_chain_head_ptr.load(std::memory_order_acquire);
    if(!current_vertex_delta_ptr)[[unlikely]]{
        return nullptr;
    }
    VertexDeltaHeader* current_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta_ptr);
    uint64_t current_ts = current_vertex_delta->get_creation_ts();
    if(is_txn_id(current_ts))[[unlikely]]{
        uint64_t status;
        if(txn_tables.get_status(current_ts,status)){
            if(status!=IN_PROGRESS){//ignore in progress ones
                if(status!=ABORT){//abort will be eager updated
                    if(current_vertex_delta->lazy_update(current_ts,status)){
                        record_lazy_update_record(& thread_specific_lazy_update_records.local(),current_ts);
                        //invalidate previous entry if exist
                        if(current_vertex_delta->get_previous_ptr()){
                            auto previous_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta->get_previous_ptr());
                            graph.get_per_thread_garbage_queue(thread_id).register_entry(current_vertex_delta->get_previous_ptr(),previous_vertex_delta->get_order(),status);
#if !USING_COMMIT_WAIT_WORK
                            if(graph.get_per_thread_garbage_queue(thread_id).need_collection()){
                                auto safe_ts = block_access_ts_table.calculate_safe_ts();
                                graph.get_per_thread_garbage_queue(thread_id).free_block(safe_ts);
                            }
#endif
                        }
                    }
                }
            }
        }
    }
    while(current_vertex_delta_ptr){
        current_ts = current_vertex_delta->get_creation_ts();
        //return the first non-abort non-in-progress delta
        if(!is_txn_id(current_ts)&&current_ts!=ABORT)[[likely]]{
            return current_vertex_delta;
        }
        current_vertex_delta_ptr = current_vertex_delta->get_previous_ptr();
        current_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta_ptr);
    }
    return nullptr;
}
#endif

/*
 * its usage should be limited. We'd like openmp thread to know its ID before invoking the function
 */
std::pair<Txn_Operation_Response, std::string_view>
SharedROTransaction::get_edge(GTX::vertex_t src, GTX::vertex_t dst, GTX::label_t label) {
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry){
        on_operation_finish();
        return std::pair<Txn_Operation_Response, std::string_view>(Txn_Operation_Response::SUCCESS,std::string_view());
    }
    auto block_id = generate_block_id(src,label);
    uint8_t thread_id = graph.get_worker_thread_id();
    if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry,block_access_ts_table)){
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        if(read_timestamp<current_block->get_creation_time()){
#if EDGE_DELTA_TEST
            if(!current_block->get_previous_ptr()){
                throw TransactionReadException();
            }
#endif
            EdgeDeltaBlockHeader* previous_block = block_manager.convert<EdgeDeltaBlockHeader>(current_block->get_previous_ptr());
            while(read_timestamp<previous_block->get_creation_time()){
#if EDGE_DELTA_TEST
                if(!previous_block->get_previous_ptr()){
                    throw TransactionReadException();
                }
#endif
                previous_block = block_manager.convert<EdgeDeltaBlockHeader>(previous_block->get_previous_ptr());
            }
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);//reading previous block needs no protection, it is protected by read epoch
            on_operation_finish();
            return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::SUCCESS,
                                                                      scan_previous_block_find_edge(previous_block,dst));
        }else{
            auto& delta_chains_index_entry = target_label_entry->delta_chain_index->at(current_block->get_delta_chain_id(dst));
            uint32_t offset = delta_chains_index_entry.get_raw_offset();//may be locked
            BaseEdgeDelta* target_delta =current_block->get_visible_target_delta_using_delta_chain(offset,dst,read_timestamp, thread_specific_lazy_update_records.local());
            if(!target_delta){
                char* data ;
                if(target_delta->data_length<=16){
                    data = target_delta->data;
                }else{
                    data = current_block->get_edge_data(target_delta->data_offset);
                }
#if EDGE_DELTA_TEST
                if(target_delta->toID!=dst){
                    throw TransactionReadException();
                }
#endif
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                on_operation_finish();
                return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::SUCCESS, std::string_view(data,target_delta->data_length));
            }else{
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                on_operation_finish();
                return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::SUCCESS,std::string());
            }
        }
    }else{
        on_operation_finish();
#if ENABLE_VERTEX_DELETION
        auto current_state = target_label_entry->state.load(std::memory_order_acquire);
        if(current_state==EdgeDeltaBlockState::DELETION)[[unlikely]]{
            return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::FAIL,std::string_view());
        }
#endif
        return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::READER_WAIT,std::string_view());
    }
}

std::pair<Txn_Operation_Response, std::string_view>
SharedROTransaction::get_edge(GTX::vertex_t src, GTX::vertex_t dst, GTX::label_t label, uint8_t thread_id) {
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry){
        on_operation_finish(thread_id);
        return std::pair<Txn_Operation_Response, std::string_view>(Txn_Operation_Response::SUCCESS,std::string_view());
    }
    auto block_id = generate_block_id(src,label);
    if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry,block_access_ts_table)){
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        if(read_timestamp<current_block->get_creation_time()){
#if EDGE_DELTA_TEST
            if(!current_block->get_previous_ptr()){
                throw TransactionReadException();
            }
#endif
            EdgeDeltaBlockHeader* previous_block = block_manager.convert<EdgeDeltaBlockHeader>(current_block->get_previous_ptr());
            while(read_timestamp<previous_block->get_creation_time()){
#if EDGE_DELTA_TEST
                if(!previous_block->get_previous_ptr()){
                    throw TransactionReadException();
                }
#endif
                previous_block = block_manager.convert<EdgeDeltaBlockHeader>(previous_block->get_previous_ptr());
            }
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);//reading previous block needs no protection, it is protected by read epoch
            on_operation_finish(thread_id);
            return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::SUCCESS,
                                                                      scan_previous_block_find_edge(previous_block,dst));
        }else{
            auto& delta_chains_index_entry = target_label_entry->delta_chain_index->at(current_block->get_delta_chain_id(dst));
            uint32_t offset = delta_chains_index_entry.get_raw_offset();//may be locked
            BaseEdgeDelta* target_delta =current_block->get_visible_target_delta_using_delta_chain(offset,dst,read_timestamp, thread_specific_lazy_update_records.local());
            if(!target_delta){
                char* data;
                if(target_delta->data_length<=16){
                    data = target_delta->data;
                }else{
                    data = current_block->get_edge_data(target_delta->data_offset);
                }
#if EDGE_DELTA_TEST
                if(target_delta->toID!=dst){
                    throw TransactionReadException();
                }
#endif
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                on_operation_finish(thread_id);
                return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::SUCCESS, std::string_view(data,target_delta->data_length));
            }else{
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                on_operation_finish(thread_id);
                return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::SUCCESS,std::string());
            }
        }
    }else{
        on_operation_finish(thread_id);
#if ENABLE_VERTEX_DELETION
        auto current_state = target_label_entry->state.load(std::memory_order_acquire);
        if(current_state==EdgeDeltaBlockState::DELETION)[[unlikely]]{
            return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::FAIL,std::string_view());
        }
#endif
        return std::pair<Txn_Operation_Response,std::string_view>(Txn_Operation_Response::READER_WAIT,std::string_view());
    }
}
/*
 * should not be used, not the simple version
 */
std::pair<Txn_Operation_Response, EdgeDeltaIterator>
SharedROTransaction::get_edges(GTX::vertex_t src, GTX::label_t label) {
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry){
        on_operation_finish();
        return std::pair<Txn_Operation_Response, EdgeDeltaIterator>(Txn_Operation_Response::SUCCESS,EdgeDeltaIterator());
    }
    uint8_t thread_id = graph.get_worker_thread_id();
    auto block_id = generate_block_id(src, label);
    if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry,block_access_ts_table)){
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        uint64_t current_combined_offset = current_block->get_current_offset();
        if(current_block->is_overflow_offset(current_combined_offset)){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            on_operation_finish();
            return std::pair<Txn_Operation_Response,EdgeDeltaIterator>(Txn_Operation_Response::READER_WAIT,EdgeDeltaIterator());
        }
        on_operation_finish();
        //block protection released by iterator destructor
        return std::pair<Txn_Operation_Response,EdgeDeltaIterator>(Txn_Operation_Response::SUCCESS,EdgeDeltaIterator(current_block,read_timestamp,((static_cast<uint64_t>(thread_id)<<56)|placeholder_txn_id),false,EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset),
                                                                                                                     graph,& thread_specific_lazy_update_records.local(),&block_access_ts_table));
    }else{
        on_operation_finish();
#if ENABLE_VERTEX_DELETION
        auto current_state = target_label_entry->state.load(std::memory_order_acquire);
        if(current_state==EdgeDeltaBlockState::DELETION)[[unlikely]]{
            return std::pair<Txn_Operation_Response,EdgeDeltaIterator>(Txn_Operation_Response::FAIL,EdgeDeltaIterator());
        }
#endif
        return std::pair<Txn_Operation_Response, EdgeDeltaIterator>(Txn_Operation_Response::READER_WAIT,EdgeDeltaIterator());
    }
}
/*
 * should not be used, not the simple version
 */
std::pair<Txn_Operation_Response, EdgeDeltaIterator>
SharedROTransaction::get_edges(GTX::vertex_t src, GTX::label_t label, uint8_t thread_id) {
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry){
        on_operation_finish(thread_id);
        return std::pair<Txn_Operation_Response, EdgeDeltaIterator>(Txn_Operation_Response::SUCCESS,EdgeDeltaIterator());
    }
    auto block_id = generate_block_id(src, label);
    if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry,block_access_ts_table)){
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        uint64_t current_combined_offset = current_block->get_current_offset();
        if(current_block->is_overflow_offset(current_combined_offset)){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            on_operation_finish(thread_id);
            return std::pair<Txn_Operation_Response,EdgeDeltaIterator>(Txn_Operation_Response::READER_WAIT,EdgeDeltaIterator());
        }
        on_operation_finish(thread_id);
        //block protection released by iterator destructor
        return std::pair<Txn_Operation_Response,EdgeDeltaIterator>(Txn_Operation_Response::SUCCESS,EdgeDeltaIterator(current_block,read_timestamp,((static_cast<uint64_t>(thread_id)<<56)|placeholder_txn_id),false,EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset),
                                                                                                                     graph,& thread_specific_lazy_update_records.local(),&block_access_ts_table));
    }else{
        on_operation_finish(thread_id);
#if ENABLE_VERTEX_DELETION
        auto current_state = target_label_entry->state.load(std::memory_order_acquire);
        if(current_state==EdgeDeltaBlockState::DELETION)[[unlikely]]{
            return std::pair<Txn_Operation_Response,EdgeDeltaIterator>(Txn_Operation_Response::FAIL,EdgeDeltaIterator());
        }
#endif
        return std::pair<Txn_Operation_Response, EdgeDeltaIterator>(Txn_Operation_Response::READER_WAIT,EdgeDeltaIterator());
    }
}
/*
 * its usage should be limited. We'd like openmp thread to know its ID before invoking the function
 */
std::pair<Txn_Operation_Response, SimpleEdgeDeltaIterator>
SharedROTransaction::simple_get_edges(GTX::vertex_t src, GTX::label_t label) {
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry){
        on_operation_finish();
        return std::pair<Txn_Operation_Response, SimpleEdgeDeltaIterator>(Txn_Operation_Response::SUCCESS,SimpleEdgeDeltaIterator());
    }
    uint8_t thread_id = graph.get_worker_thread_id();
    //std::cout<<static_cast<uint32_t>(thread_id)<<std::endl;
    //uint8_t thread_id = graph.get_openmp_worker_thread_id();
    auto block_id = generate_block_id(src, label);
    if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry,block_access_ts_table)){
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        uint64_t current_combined_offset = current_block->get_current_offset();
        if(current_block->is_overflow_offset(current_combined_offset)){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            on_operation_finish();
            return std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator>(Txn_Operation_Response::READER_WAIT,SimpleEdgeDeltaIterator());
        }
        //block protection released by iterator close
        on_operation_finish();
        return std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator>(Txn_Operation_Response::SUCCESS,SimpleEdgeDeltaIterator(current_block,read_timestamp,((static_cast<uint64_t>(thread_id)<<56)|placeholder_txn_id),EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset),
                                                                                                                                 graph,& thread_specific_lazy_update_records.local(),&block_access_ts_table));
    }else{
        on_operation_finish();
#if ENABLE_VERTEX_DELETION
        auto current_state = target_label_entry->state.load(std::memory_order_acquire);
        if(current_state==EdgeDeltaBlockState::DELETION)[[unlikely]]{
            return std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator>(Txn_Operation_Response::FAIL,SimpleEdgeDeltaIterator());
        }
#endif
        return std::pair<Txn_Operation_Response, SimpleEdgeDeltaIterator>(Txn_Operation_Response::READER_WAIT,SimpleEdgeDeltaIterator());
    }
}
/*
 * use this version, use the simplified iterator and pass thread id as an argument
 */
std::pair<Txn_Operation_Response, SimpleEdgeDeltaIterator>
SharedROTransaction::simple_get_edges(GTX::vertex_t src, GTX::label_t label, uint8_t thread_id) {
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry){
        on_operation_finish(thread_id);
        return std::pair<Txn_Operation_Response, SimpleEdgeDeltaIterator>(Txn_Operation_Response::SUCCESS,SimpleEdgeDeltaIterator());
    }
    auto block_id = generate_block_id(src, label);
    if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry,block_access_ts_table)){
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        uint64_t current_combined_offset = current_block->get_current_offset();
        if(current_block->is_overflow_offset(current_combined_offset)){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            on_operation_finish(thread_id);
            return std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator>(Txn_Operation_Response::READER_WAIT,SimpleEdgeDeltaIterator());
        }
        //block protection released by iterator close
        on_operation_finish(thread_id);
        return std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator>(Txn_Operation_Response::SUCCESS,SimpleEdgeDeltaIterator(current_block,read_timestamp,((static_cast<uint64_t>(thread_id)<<56)|placeholder_txn_id),EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset),
                                                                                                                                 graph,& thread_specific_lazy_update_records.local(),&block_access_ts_table));
    }else{
        on_operation_finish(thread_id);
#if ENABLE_VERTEX_DELETION
        auto current_state = target_label_entry->state.load(std::memory_order_acquire);
        if(current_state==EdgeDeltaBlockState::DELETION)[[unlikely]]{
            return std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator>(Txn_Operation_Response::FAIL,SimpleEdgeDeltaIterator());
        }
#endif
        return std::pair<Txn_Operation_Response, SimpleEdgeDeltaIterator>(Txn_Operation_Response::READER_WAIT,SimpleEdgeDeltaIterator());
    }
}
/*
 * this function passes edge iterator as an argument to reused the object
 */
Txn_Operation_Response
SharedROTransaction::simple_get_edges(GTX::vertex_t src, GTX::label_t label, uint8_t thread_id,
                                      std::unique_ptr<GTX::SimpleEdgeDeltaIterator> &edge_iterator) {
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry)[[unlikely]]{
        on_operation_finish(thread_id);
        return Txn_Operation_Response::SUCCESS;
    }
    auto block_id = generate_block_id(src, label);
    if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry,block_access_ts_table))[[likely]]{
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        uint64_t current_combined_offset = current_block->get_current_offset();
        if(current_block->is_overflow_offset(current_combined_offset))[[unlikely]]{
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            on_operation_finish(thread_id);
            return Txn_Operation_Response::READER_WAIT;
        }
        on_operation_finish(thread_id);
        edge_iterator->fill_new_iterator(current_block,EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset));
        return Txn_Operation_Response::SUCCESS;
    }else{
        on_operation_finish(thread_id);
#if ENABLE_VERTEX_DELETION
        auto current_state = target_label_entry->state.load(std::memory_order_acquire);
        if(current_state==EdgeDeltaBlockState::DELETION)[[unlikely]]{
            return Txn_Operation_Response::FAIL;
        }
#endif
        return Txn_Operation_Response::READER_WAIT;
    }
}

std::string_view SharedROTransaction::scan_previous_block_find_edge(GTX::EdgeDeltaBlockHeader *previous_block,
                                                                    GTX::vertex_t vid) {
    uint64_t combined_offset = previous_block->get_current_offset();
    uint32_t current_delta_offset = EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(combined_offset);
    BaseEdgeDelta* current_delta = previous_block->get_edge_delta(current_delta_offset);
    while(current_delta_offset>0){
        if(current_delta->toID==vid){
            uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
            //abort and txn id are both quite large
            if(original_ts<=read_timestamp){
                if(current_delta->data_length<=16){
                    return std::string_view (current_delta->data,current_delta->data_length);
                }else{
                    return std::string_view (previous_block->get_edge_data(current_delta->data_offset),current_delta->data_length);
                }
            }
        }
        current_delta ++;
        current_delta_offset-=ENTRY_DELTA_SIZE;
    }
    return std::string_view();
}
//todo: the following functions need to accomodate vertex deletions
uint64_t SharedROTransaction::get_neighborhood_size(GTX::vertex_t src, GTX::label_t label, uint8_t thread_id) {
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry)[[unlikely]]{
        on_operation_finish(thread_id);
        return std::numeric_limits<uint64_t>::max();
    }
    auto block_id = generate_block_id(src, label);
    uint64_t result = 0;
    while(true){
        if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry,block_access_ts_table))[[likely]]{
            auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
            on_operation_finish(thread_id);
            //read current block
            if(current_block->get_creation_time()<=read_timestamp)[[likely]]{
                uint64_t current_combined_offset = current_block->get_current_offset();
                if(current_block->is_overflow_offset(current_combined_offset))[[unlikely]]{
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    on_operation_finish(thread_id);
                    continue;
                }
                //start the scan
                auto current_delta_offset = GTX::EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset);
                auto current_delta = current_block->get_edge_delta(current_delta_offset);
                while(current_delta_offset>0){
                    //need lazy update
                    auto original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                    if(original_ts==0)[[unlikely]]{
                        current_delta_offset-=ENTRY_DELTA_SIZE;
                        current_delta++;
                        continue;
                    }
                    //lazy update
                    if(is_txn_id(original_ts))[[unlikely]]{
                        uint64_t status = 0;
                        if(txn_tables.get_status(original_ts,status))[[likely]]{
                            if(status == IN_PROGRESS){
                                current_delta_offset -= ENTRY_DELTA_SIZE;
                                current_delta++;
                                continue;
                            }else{
                                if(status!=ABORT){
                                    current_block->update_previous_delta_invalidate_ts(current_delta->toID,
                                                                                       current_delta->previous_version_offset,
                                                                                       status);
                                    if (current_delta->lazy_update(original_ts, status)) {
                                        //record lazy update
                                        txn_tables.reduce_op_count(original_ts,1);
                                    }
                                }
#if EDGE_DELTA_TEST
                                if(current_delta->creation_ts.load(std::memory_order_acquire)!=status){
                                    throw LazyUpdateException();
                                }
#endif
                                original_ts = status;
                            }
                        }else{
                            original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                        }
                    }
                    if(original_ts<=read_timestamp){
                        if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                            auto invalidation_ts = current_delta->invalidate_ts.load(std::memory_order_acquire);
                            if(invalidation_ts==0||invalidation_ts>read_timestamp){
                                result++;
                            }
                        }
                    }
                    current_delta_offset -= ENTRY_DELTA_SIZE;
                    current_delta++;
                }
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            }else{//read previous block
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                bool found = false;
                while (current_block->get_previous_ptr()) {
                    current_block = block_manager.convert<EdgeDeltaBlockHeader>(
                            current_block->get_previous_ptr());
                    if (read_timestamp >= current_block->get_creation_time()) {
                        found = true;
                        break;
                    }
                }
                if(found)[[likely]]{
                    auto previous_block_offset = current_block->get_current_offset();
                    auto current_delta_offset = static_cast<uint32_t>(previous_block_offset & SIZE2MASK);
                    auto current_delta = current_block->get_edge_delta(current_delta_offset);
                    while(current_delta_offset>0){
                        if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                            auto original_ts = current_delta->creation_ts.load(std::memory_order_relaxed);
                            if(original_ts<=read_timestamp){
                                auto invalidation_ts = current_delta->invalidate_ts.load(std::memory_order_relaxed);
                                if(invalidation_ts==0||invalidation_ts>read_timestamp){
                                    result++;
                                }
                            }
                        }
                        current_delta_offset -= ENTRY_DELTA_SIZE;
                        current_delta++;
                    }
                }
            }
            //will return if we get the access
            return result;
        }
        //do nothing
    }
    throw std::runtime_error("unreachable");
}

int64_t SharedROTransaction::get_neighborhood_size_signed(GTX::vertex_t src, GTX::label_t label,
                                                          uint8_t thread_id) {
    BwLabelEntry* target_label_entry = reader_access_label(src,label);
    if(!target_label_entry)[[unlikely]]{
        on_operation_finish(thread_id);
        return std::numeric_limits<int64_t>::max();
    }
    auto block_id = generate_block_id(src, label);
    int64_t result = 0;
    while(true){
        if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry,block_access_ts_table))[[likely]]{
            auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
            on_operation_finish(thread_id);
            //read current block
            if(current_block->get_creation_time()<=read_timestamp)[[likely]]{
                uint64_t current_combined_offset = current_block->get_current_offset();
                if(current_block->is_overflow_offset(current_combined_offset))[[unlikely]]{
                    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    on_operation_finish(thread_id);
                    continue;
                }
                //start the scan
                auto current_delta_offset = GTX::EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset);
                auto current_delta = current_block->get_edge_delta(current_delta_offset);
                while(current_delta_offset>0){
                    //need lazy update
                    auto original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                    if(original_ts==0)[[unlikely]]{
                        current_delta_offset-=ENTRY_DELTA_SIZE;
                        current_delta++;
                        continue;
                    }
                    //lazy update
                    if(is_txn_id(original_ts))[[unlikely]]{
                        uint64_t status = 0;
                        if(txn_tables.get_status(original_ts,status))[[likely]]{
                            if(status == IN_PROGRESS){
                                current_delta_offset -= ENTRY_DELTA_SIZE;
                                current_delta++;
                                continue;
                            }else{
                                if(status!=ABORT){
                                    current_block->update_previous_delta_invalidate_ts(current_delta->toID,
                                                                                       current_delta->previous_version_offset,
                                                                                       status);
                                    if (current_delta->lazy_update(original_ts, status)) {
                                        //record lazy update
                                        txn_tables.reduce_op_count(original_ts,1);
                                    }
                                }
#if EDGE_DELTA_TEST
                                if(current_delta->creation_ts.load(std::memory_order_acquire)!=status){
                                    throw LazyUpdateException();
                                }
#endif
                                original_ts = status;
                            }
                        }else{
                            original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                        }
                    }
                    if(original_ts<=read_timestamp){
                        if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                            auto invalidation_ts = current_delta->invalidate_ts.load(std::memory_order_acquire);
                            if(invalidation_ts==0||invalidation_ts>read_timestamp){
                                result++;
                            }
                        }
                    }
                    current_delta_offset -= ENTRY_DELTA_SIZE;
                    current_delta++;
                }
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            }else{//read previous block
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                bool found = false;
                while (current_block->get_previous_ptr()) {
                    current_block = block_manager.convert<EdgeDeltaBlockHeader>(
                            current_block->get_previous_ptr());
                    if (read_timestamp >= current_block->get_creation_time()) {
                        found = true;
                        break;
                    }
                }
                if(found)[[likely]]{
                    auto previous_block_offset = current_block->get_current_offset();
                    auto current_delta_offset = static_cast<uint32_t>(previous_block_offset & SIZE2MASK);
                    auto current_delta = current_block->get_edge_delta(current_delta_offset);
                    while(current_delta_offset>0){
                        if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                            auto original_ts = current_delta->creation_ts.load(std::memory_order_relaxed);
                            if(original_ts<=read_timestamp){
                                auto invalidation_ts = current_delta->invalidate_ts.load(std::memory_order_relaxed);
                                if(invalidation_ts==0||invalidation_ts>read_timestamp){
                                    result++;
                                }
                            }
                        }
                        current_delta_offset -= ENTRY_DELTA_SIZE;
                        current_delta++;
                    }
                }
            }
            //will return if we get the access
            return result;
        }
        //do nothing
    }
    throw std::runtime_error("unreachable");
}

/*
 * get total number of edges of a certain label
 */
uint64_t SharedROTransaction::get_total_edge_num(GTX::label_t label) {
    std::atomic_uint64_t total_edge_num = 0;
    const uint64_t max_vertex_id = graph.get_max_allocated_vid();
#pragma omp parallel
    {
        uint8_t thread_id = graph.get_openmp_worker_thread_id();
        uint64_t private_edge_num = 0;
#pragma omp for
        for (uint64_t src = 1; src <= max_vertex_id; src++) {
            BwLabelEntry* target_label_entry = reader_access_label(src,label);
            if(!target_label_entry)[[unlikely]]{
                on_operation_finish(thread_id);
                continue;
            }
            auto block_id = generate_block_id(src, label);
            while(true){
                if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry,block_access_ts_table))[[likely]]{
                    auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
                    on_operation_finish(thread_id);
                    //read current block
                    if(current_block->get_creation_time()<=read_timestamp)[[likely]]{
                        uint64_t current_combined_offset = current_block->get_current_offset();
                        if(current_block->is_overflow_offset(current_combined_offset))[[unlikely]]{
                            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                            on_operation_finish(thread_id);
                            continue;
                        }
                        //start the scan
                        auto current_delta_offset = GTX::EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset);
                        auto current_delta = current_block->get_edge_delta(current_delta_offset);
                        while(current_delta_offset>0){
                            //need lazy update
                            auto original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                            if(original_ts==0)[[unlikely]]{
                                current_delta_offset-=ENTRY_DELTA_SIZE;
                                current_delta++;
                                continue;
                            }
                            //lazy update
                            if(is_txn_id(original_ts))[[unlikely]]{
                                uint64_t status = 0;
                                if(txn_tables.get_status(original_ts,status))[[likely]]{
                                    if(status == IN_PROGRESS){
                                        current_delta_offset -= ENTRY_DELTA_SIZE;
                                        current_delta++;
                                        continue;
                                    }else{
                                        if(status!=ABORT){
                                            current_block->update_previous_delta_invalidate_ts(current_delta->toID,
                                                                                               current_delta->previous_version_offset,
                                                                                               status);
                                            if (current_delta->lazy_update(original_ts, status)) {
                                                //record lazy update
                                                txn_tables.reduce_op_count(original_ts,1);
                                            }
                                        }
#if EDGE_DELTA_TEST
                                        if(current_delta->creation_ts.load(std::memory_order_acquire)!=status){
                                    throw LazyUpdateException();
                                }
#endif
                                        original_ts = status;
                                    }
                                }else{
                                    original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                                }
                            }
                            if(original_ts<=read_timestamp){
                                if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                                    auto invalidation_ts = current_delta->invalidate_ts.load(std::memory_order_acquire);
                                    if(invalidation_ts==0||invalidation_ts>read_timestamp){
                                        private_edge_num++;
                                    }
                                }
                            }
                            current_delta_offset -= ENTRY_DELTA_SIZE;
                            current_delta++;
                        }
                        BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                    }else{//read previous block
                        BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                        bool found = false;
                        while (current_block->get_previous_ptr()) {
                            current_block = block_manager.convert<EdgeDeltaBlockHeader>(
                                    current_block->get_previous_ptr());
                            if (read_timestamp >= current_block->get_creation_time()) {
                                found = true;
                                break;
                            }
                        }
                        if(found)[[likely]]{
                            auto previous_block_offset = current_block->get_current_offset();
                            auto current_delta_offset = static_cast<uint32_t>(previous_block_offset & SIZE2MASK);
                            auto current_delta = current_block->get_edge_delta(current_delta_offset);
                            while(current_delta_offset>0){
                                if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                                    auto original_ts = current_delta->creation_ts.load(std::memory_order_relaxed);
                                    if(original_ts<=read_timestamp){
                                        auto invalidation_ts = current_delta->invalidate_ts.load(std::memory_order_relaxed);
                                        if(invalidation_ts==0||invalidation_ts>read_timestamp){
                                            private_edge_num++;
                                        }
                                    }
                                }
                                current_delta_offset -= ENTRY_DELTA_SIZE;
                                current_delta++;
                            }
                        }
                    }
                    break;
                    //will return if we get the access
                    //return result;
                }
                //do nothing
            }
        }
        total_edge_num.fetch_add(private_edge_num,std::memory_order_acq_rel);
        thread_on_openmp_section_finish(thread_id);
    }
    graph.on_openmp_parallel_session_finish();
    return total_edge_num.load(std::memory_order_relaxed)/2;
}

EdgeDeltaBlockHeader *SharedROTransaction::get_block_header(uint64_t vid, GTX::label_t label, uint8_t thread_id,
                                                            uint32_t *current_delta_offset) {
    BwLabelEntry *target_label_entry = reader_access_label(vid, label);
    if (!target_label_entry)[[unlikely]] {
        on_operation_finish(thread_id);
        return nullptr;
    }
    auto block_id = generate_block_id(vid, label);
    while(true){
        if (BlockStateVersionProtectionScheme::reader_access_block(thread_id, block_id, target_label_entry,
                                                                   block_access_ts_table))[[likely]] {
            auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
            auto current_combined_offset = current_block->get_current_offset();
            if(!current_block->is_overflow_offset(current_combined_offset))[[likely]]{
                *current_delta_offset = current_block->get_delta_offset_from_combined_offset(current_combined_offset);
                return current_block;
            }else{
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            }
        }
#if ENABLE_VERTEX_DELETION
        else{
            auto current_state = target_label_entry->state.load();
            if(current_state==EdgeDeltaBlockState::DELETION){
                throw VertexDeltaException();
            }
        }
#endif
    }
    throw std::runtime_error("should never reach here");
}
//demo version with only 1 label property graph
bool DeleteTransaction::put_delete_vertex_delta(VertexIndexEntry & vertex_entry) {
    uintptr_t current_vertex_delta_ptr;
    VertexDeltaHeader* current_vertex_delta =nullptr;
    timestamp_t original_ts = 0;
    while(true){
        current_vertex_delta_ptr = vertex_entry.vertex_delta_chain_head_ptr.load(std::memory_order_acquire);
        if(current_vertex_delta_ptr){
            current_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta_ptr);
            /*timestamp_t*/ original_ts = current_vertex_delta->get_creation_ts();
            if(original_ts==local_txn_id){
                break;
            }
            if(is_txn_id(original_ts)){
                uint64_t status;
                if(txn_tables.get_status(original_ts,status)){
                    if(status==IN_PROGRESS){
                        return false;
                    }else if(status!=ABORT){
                        if(current_vertex_delta->lazy_update(original_ts,status)){
                            record_lazy_update_record(&lazy_update_records,original_ts);
                            //invalidate previous entry if exist
                            if(current_vertex_delta->get_previous_ptr()){
                                auto previous_vertex_delta = block_manager.convert<VertexDeltaHeader>(current_vertex_delta->get_previous_ptr());
                                per_thread_garbage_queue.register_entry(current_vertex_delta->get_previous_ptr(),previous_vertex_delta->get_order(),status);
#if !USING_COMMIT_WAIT_WORK
                                if(per_thread_garbage_queue.need_collection()){
                                    auto safe_ts = block_access_ts_table.calculate_safe_ts();
                                    per_thread_garbage_queue.free_block(safe_ts);
                                }
#endif
                            }
                        }
#if TXN_TEST
                        if(current_vertex_delta->get_creation_ts()!=status){
                            throw VertexDeltaException();
                        }
#endif
                        if(status>read_timestamp){
                            return false;
                        }else{
                            break;
                        }
                    }else{
                        continue; //status == abort, eager abort is done, we need to reread the entry
                    }
                }else{
                    continue;//no status so delta is lazy updated
                }
            }else if(original_ts!=ABORT){
                if(original_ts>read_timestamp){
                    return false;
                }
                break;
            }else{
                continue;//don't worry because of eager abort, continue and reload delta until we see a committed version or 0
            }
        }else{
            break;
        }
    }
    //now put the special delete delta, it is a vertex delta with creation_time infinity
    order_t new_order = size_to_order(sizeof(VertexDeltaHeader)+20);
    auto new_delta_ptr = block_manager.alloc(new_order);
    auto new_vertex_delta = block_manager.convert<VertexDeltaHeader>(new_delta_ptr);
    if(current_vertex_delta_ptr)[[likely]]{
        new_vertex_delta->fill_metadata(std::numeric_limits<uint64_t>::max(),0,new_order,current_vertex_delta_ptr);

        if(vertex_entry.install_vertex_delta(current_vertex_delta_ptr,new_delta_ptr)){
            op_count++;
            return true;
        }else{
            auto zero_out_ptr = block_manager.convert<uint8_t>(new_delta_ptr);
            memset(zero_out_ptr,'\0',1ul<<new_order);
            block_manager.free(new_delta_ptr,new_order);
            return false;
        }
    }else{
        throw std::runtime_error("error, must delete an existing vertex");
    }
}
void DeleteTransaction::delete_vertex(vertex_t vid) {
    to_delete_vid = vid;
    //locate the vertex;
    auto& vertex_index_entry = graph.get_vertex_index_entry(to_delete_vid);
    //cannot insert to invalid vertex entry
    if(!vertex_index_entry.valid.load(std::memory_order_acquire)){
        return;
    }
    //step 1: put vertex deletion delta
    while(put_delete_vertex_delta(vertex_index_entry)){
        usleep(1000000);
        read_timestamp = commit_manager.get_current_read_ts();
    }

    auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
    BwLabelEntry* target_label_entry;//get the label
    edge_label_block->reader_lookup_label(1,target_label_entry);
    uint64_t block_id = generate_block_id(to_delete_vid,1);

    if(!target_label_entry){
        //todo:: this should not happen?
        // std::cout<<"should never happen"<<std::endl;
        return;
    }
    //now just register access and change state
    auto current_state = BlockStateVersionProtectionScheme::writer_access_block_return_state(thread_id,block_id,target_label_entry,block_access_ts_table);
    //Delete has to start with transaction registering access. Only the blocks in normal state can be deleted. Otherwise we just wait till the block is consolidated.
    while(current_state!=EdgeDeltaBlockState::NORMAL){
        //sleep or loop or do other useful work, then comeback to it
        usleep(1000000);
        current_state = BlockStateVersionProtectionScheme::writer_access_block_return_state(thread_id,block_id,target_label_entry,block_access_ts_table);
    }
    //Put the vertex to delete state: all other transactions that need to enter this vertex by checking the state will abort. We are eager in the logical deletion.
    auto install_result = BlockStateVersionProtectionScheme::install_exclusive_delete_state(thread_id,block_id,target_label_entry,block_access_ts_table);
    //wait till concurrent committing transactions and accessing transactions to exit.
    while(!install_result){
        //sleep or loop or do other useful work, then comeback to it, wait for consolidation to be done
        usleep(5000000);
        install_result = BlockStateVersionProtectionScheme::install_exclusive_delete_state(thread_id,block_id,target_label_entry,block_access_ts_table);
    }
    //scan and analyze the block, participate lazy update
    auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
    auto combined_offset = current_block->get_current_offset();
    auto current_delta_offset = EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(combined_offset);
    auto current_delta = current_block->get_edge_delta(current_delta_offset);//start scanning
    //in progress delta, and committed delta
    std::set<uint64_t> to_delete_deltas;
    std::unordered_map<uint64_t, uint32_t> in_progress_deltas;
    std::unordered_map<vertex_t ,commit_delta_pair> to_commit_deltas;
    auto total_offset = current_delta_offset;
    while(total_offset>0){
        auto current_id_ts = current_delta->creation_ts.load(std::memory_order_acquire);
        //lazy update
        if(is_txn_id(current_id_ts)){
            uint64_t status = 0;
            if(txn_tables.get_status(current_id_ts,status))[[likely]]{
                if(status == IN_PROGRESS){
                    //record in progress delta, and continue
                    in_progress_deltas.try_emplace(current_id_ts,current_delta_offset);
                    current_delta_offset -= ENTRY_DELTA_SIZE;
                    current_delta++;
                    continue;
                }else{
                    if(status!=ABORT){
                        current_block->update_previous_delta_invalidate_ts(current_delta->toID,
                                                                           current_delta->previous_version_offset,
                                                                           status);
                        if (current_delta->lazy_update(current_id_ts, status)) {
                            //record lazy update
                            txn_tables.reduce_op_count(current_id_ts,1);
                        }
                    }
#if EDGE_DELTA_TEST
                    if(current_delta->creation_ts.load(std::memory_order_acquire)!=status){
                                    throw LazyUpdateException();
                                }
#endif
                    current_id_ts = status;
                }
            }else{
                current_id_ts = current_delta->creation_ts.load(std::memory_order_acquire);
            }
        }
        if(current_id_ts!=ABORT){
            to_delete_deltas.emplace(current_delta->toID);//record to delete deltas
        }
        total_offset-=64;
    }
    //the analysis is done, now wait for all in progress deltas to finalize. Concurrent transactions that do not pass the validation phase will abort.
    while(!in_progress_deltas.empty()){
        for (auto it =in_progress_deltas.begin();it!=in_progress_deltas.end();){
            current_delta = current_block->get_edge_delta(it->second);
            auto current_id_ts = current_delta->creation_ts.load(std::memory_order_acquire);
            if(is_txn_id(current_id_ts)){//lazy update check again
                uint64_t status = 0;
                if(txn_tables.get_status(current_id_ts,status))[[likely]]{
                    //if in progress, move on and come back later
                    if(status == IN_PROGRESS){
                        it++;
                        continue;
                    }else{
                        if(status!=ABORT){
                            current_block->update_previous_delta_invalidate_ts(current_delta->toID,
                                                                               current_delta->previous_version_offset,
                                                                               status);
                            if (current_delta->lazy_update(current_id_ts, status)) {
                                //record lazy update
                                txn_tables.reduce_op_count(current_id_ts,1);
                            }
                        }
#if EDGE_DELTA_TEST
                        if(current_delta->creation_ts.load(std::memory_order_acquire)!=status){
                                    throw LazyUpdateException();
                                }
#endif
                        current_id_ts = status;
                    }
                }else{
                    current_id_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                }
            }
            if(current_id_ts!=ABORT){
                to_delete_deltas.emplace(current_delta->toID);
            }
            //we are done with this entry
            in_progress_deltas.erase(it);
        }
    }
    //now we have resolved all committing transactions. Only the transactions that validate before we enter the exclusive delete can commit.
    //now go to delete all reverse edges. Since we are exclusively holding the vertex lock, no edges can be added for the reverse edge.
    std::set<vertex_t > to_delete_deltas_commit_copies(to_delete_deltas);
    while(!to_delete_deltas.empty()){
        for(auto it = to_delete_deltas.begin();it!=to_delete_deltas.end();){
            if(delete_reverse_edge(*it,to_commit_deltas)){
                //todo: log this reverse edge deletion
                op_count++;
                to_delete_deltas.erase(it);
            }else{
                it++;
            }
        }
    }
    //todo:: add a vertex deletion delta

    //now enter commit phase
    self_entry->op_count.store(op_count,std::memory_order_release);
    commit_manager.txn_commit(thread_id,self_entry,true);
    while(!self_entry->status.load(std::memory_order_acquire));//loop until committed
    //eager installation of deletion
    eager_clean_reverse_edge_block(to_commit_deltas);
    //now all delete edge deltas are good, we need to do post progressing to recycle this vertex entry
    //wait till safe ts to be large enough, then mark the vertex index entry as invalid, recycle all vertex, label and edge blocks, and allow current thread to reuse this vertex ID.
    timestamp_t commit_ts = self_entry->status;
    block_access_ts_table.store_current_ts(thread_id,commit_ts);
    auto safe_ts = block_access_ts_table.calculate_safe_ts();
    while(safe_ts!=commit_ts){
        safe_ts = block_access_ts_table.calculate_safe_ts();
    }
    //now remove the entry
    delete target_label_entry->delta_chain_index;//delete delta chains index
    block_manager.free(vertex_index_entry.edge_label_block_ptr,size_to_order(sizeof(EdgeLabelBlock)));//free edge label block
    block_manager.free(vertex_index_entry.vertex_delta_chain_head_ptr,size_to_order(sizeof(VertexDeltaHeader)+20));//free delete vertex delta
    //recycle the vertex ID
    thread_local_recycled_vertices.push(to_delete_vid);
    vertex_index_entry.valid.store(false);
}

bool DeleteTransaction::delete_reverse_edge(GTX::vertex_t other_vid,std::unordered_map<vertex_t,commit_delta_pair>& to_commit_deltas) {
    //todo:: this function should stop the block from consolidation, we install the holder while doing the delete. So update the offset to point to us, and have the lock.
    //locate the vertex;
    auto& vertex_index_entry = graph.get_vertex_index_entry(other_vid);
    //cannot insert to invalid vertex entry
    if(!vertex_index_entry.valid.load(std::memory_order_acquire)){
        return true;
    }

    auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
    BwLabelEntry* target_label_entry;//get the label
    edge_label_block->reader_lookup_label(1,target_label_entry);
    uint64_t block_id = generate_block_id(other_vid,1);

    if(!target_label_entry){
        //todo:: this should not happen?
        // std::cout<<"should never happen"<<std::endl;
        return true;
    }
    //now just register access and change state
    auto current_state = BlockStateVersionProtectionScheme::writer_access_block_return_state(thread_id,block_id,target_label_entry,block_access_ts_table);
    if(current_state!=EdgeDeltaBlockState::NORMAL){
        return false;//if block under consolidation, return false and move forward.
    }
    //now start deleting the edge
    auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
    if(current_block->already_overflow())[[unlikely]]{
        BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
        return false;//if block already overflow, return false and move forward.
    }
    int32_t total_delta_chain_num = current_block->get_delta_chain_num();
    uint32_t current_delta_chain_head_offset = 0;
    delta_chain_id_t target_delta_chain_id = calculate_owner_delta_chain_id(to_delete_vid,total_delta_chain_num);
    auto lock_result = current_block->simple_set_protection_on_delta_chain(target_delta_chain_id,&lazy_update_records,read_timestamp,&current_delta_chain_head_offset);
    if(lock_result==Delta_Chain_Lock_Response::CONFLICT)[[unlikely]]{
        BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
        return false;//comeback later
    }else if(lock_result == Delta_Chain_Lock_Response::UNCLEAR)[[unlikely]]{
        BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
        return false;//come back later
    }
    auto allocate_delta_result = allocate_delta(current_block, 0);
    if(allocate_delta_result==EdgeDeltaInstallResult::SUCCESS)[[likely]]{
        uint32_t previous_version_offset=0;
        if(current_delta_chain_head_offset)[[likely]]{
            previous_version_offset = current_block->fetch_previous_version_offset(to_delete_vid,current_delta_chain_head_offset,local_txn_id,lazy_update_records);
        }else{
            throw std::runtime_error("previous version must exist");
        }
        if(previous_version_offset==0)[[unlikely]]{
            throw std::runtime_error("previous version must exist");
        }

        current_block->checked_append_edge_delta(to_delete_vid,local_txn_id,EdgeDeltaType::DELETE_DELTA, nullptr,0,current_delta_chain_head_offset,previous_version_offset, current_delta_offset,current_data_offset);
        to_commit_deltas.try_emplace(other_vid,current_delta_offset,previous_version_offset);
        //we do validation immediately.
        current_block->deletion_immediate_validation(target_delta_chain_id,current_delta_offset);
    }else{
        BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
        return false;
    }
    //if we reach here, we have deleted the previous version and can return true now
    BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
    return true;
}

void DeleteTransaction::eager_clean_reverse_edge_block(std::unordered_map<vertex_t,commit_delta_pair>& to_commit_deltas) {
    timestamp_t commit_ts = self_entry->status.load(std::memory_order_acquire);
    BaseEdgeDelta* current_delta;
    BaseEdgeDelta* previous_delta;
    //1 block 1 entry
    for(auto it : to_commit_deltas){
        //check each block, if state is overflow and installation, we just move on
        auto vid = it.first;
        auto& vertex_index_entry = graph.get_vertex_index_entry(vid);
        //cannot insert to invalid vertex entry
        if(!vertex_index_entry.valid.load(std::memory_order_acquire)){
            throw std::runtime_error("error, the vertex must exist");
        }
        //add protection
        auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
        BwLabelEntry* target_label_entry;//get the label
        edge_label_block->reader_lookup_label(1,target_label_entry);
        uint64_t block_id = generate_block_id(vid,1);
        if(BlockStateVersionProtectionScheme::reader_access_block(thread_id,block_id,target_label_entry,block_access_ts_table)){
            auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
            current_delta = current_block->get_edge_delta(it.second.new_delta_offset);
            timestamp_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
            if(original_ts==local_txn_id){
                previous_delta = current_block->get_edge_delta(it.second.previous_delta_offset);
                previous_delta->invalidate_ts.store(commit_ts);
                if(current_delta->lazy_update(original_ts,commit_ts)){
                    self_entry->op_count.fetch_sub(1,std::memory_order_acq_rel);
                }
            }
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
        }
    }
}
#endif