#pragma once
// ref: Anthony Williams -Multithreading c++11 extensions
#include <iostream>
#include <vector>
#include <thread>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <cstdlib>

template<typename T>
class threadsafe_queue
{
private:
     threadsafe_queue(const threadsafe_queue& other)
     {
     };
     threadsafe_queue& operator=(const threadsafe_queue& other)
     {
     };

    struct node
    {
        std::shared_ptr<T> data;
        std::unique_ptr<node> next;
    };

    std::condition_variable data_cond;
    std::mutex              head_mutex;
    std::unique_ptr<node>   head;
    std::mutex              tail_mutex;
    node*                   tail;
  
    node* get_tail()
    {
        std::lock_guard<std::mutex> tail_lock(tail_mutex);
        return tail;
    }

       
public:
    threadsafe_queue(): head(new node),tail(head.get())
    {
	
    }
 
    void pop(T& value)
    {
            std::unique_lock<std::mutex> head_lock(head_mutex);
            data_cond.wait(head_lock, [&] {return (head.get() != get_tail());} );
            value = std::move(*head->data);
            std::unique_ptr<node> const old_head = std::move(head);
            head = std::move(old_head->next);
    }

    std::shared_ptr<T> try_pop()
    {
        std::unique_lock<std::mutex> head_lock(head_mutex);
        data_cond.wait(head_lock, [&] {return (head.get() != get_tail());} );

        std::unique_ptr<node> old_head  = std::move(head);
        head	= std::move( old_head->next );
		
        return ((old_head) ? old_head->data : std::shared_ptr<T>() ); 
    }
    
    void push_back(T new_value)
    {
        std::shared_ptr<T> new_data( std::make_shared<T>( std::move(new_value) ));
        
		std::unique_ptr<node> pNewNode(new node);
		
		node* const new_tail	=	pNewNode.get();
		{
			std::lock_guard<std::mutex> tail_lock(tail_mutex);
			tail->data	= new_data;
			tail->next	= std::move(pNewNode);
			tail		= new_tail;
		}

	    data_cond.notify_one();
    }

    void push_front(T new_value)
    {
        std::shared_ptr<T> new_data( std::make_shared<T>( std::move(new_value) ));
    	std::unique_ptr<node>  old_head = std::move(head);

		std::unique_ptr<node> pNewNode(new node);
		node*  new_head	=	pNewNode.get();
		{
			std::lock_guard<std::mutex> head_lock(head_mutex);
			
			new_head->data	= new_data;
			new_head->next  = std::move(old_head);
			head			= std::move(pNewNode);
		}
	    data_cond.notify_one(); //notify_all for many case
    }

};

