require 'sidekiq/fetch'

CACHE_VALIDITY_SECONDS = 5

module Sidekiq
  module DynamicQueues

    # enable with:
    #    Sidekiq.configure_server do |config|
    #        config.options[:fetch] = Sidekiq::DynamicQueues::Fetch
    #    end
    #
    class Fetch < Sidekiq::BasicFetch
      include Sidekiq::DynamicQueues::Attributes

      def initialize(options)
        super
        @dynamic_queues = self.class.translate_from_cli(*options[:queues])

        @cache_updated = 0
        @cached_queues = []
      end

      # overriding Sidekiq::BasicFetch#queues_cmd
      # Note: strictly ordered not supported.
      def queues_cmd
        if @dynamic_queues.grep(/(^!)|(^@)|(\*)/).size == 0
          super
        else
          queues = if cache_valid
                     @cached_queues
                   else
                     update_cache(expand_queues(@dynamic_queues))
                   end
          queues = queues.shuffle
          queues << Sidekiq::Fetcher::TIMEOUT
        end
      end

      def self.translate_from_cli(*queues)
        queues.collect do |queue|
          queue.gsub('.star.', '*').gsub('.at.', '@').gsub('.not.', '!')
        end
      end

      private
      def cache_valid
        Time.now.to_i - @cache_updated < CACHE_VALIDITY_SECONDS
      end

      def update_cache(queues)
        @cached_queues = queues
        @cache_updated = Time.now.to_i

        queues
      end
    end
  end
end
